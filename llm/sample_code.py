# -*- coding: utf-8 -*-
import datetime
import json
from typing import Optional, Union

from sqlalchemy import or_
from sqlalchemy.orm import Session, Query
from sqlalchemy.sql.elements import BinaryExpression

from apps.const import Tsinghua
from apps.edu.models import (
    GraphTaskTypeEnum,
    GraphTaskModel,
    GraphTaskStatusEnum,
    TaskDataSchema,
    GraphImportSchema,
    GraphModel,
    GraphTypeEnum,
    GraphStatusEnum,
    GraphToGraphModel,
    GraphFilesModel,
    NodeTextModel,
    ConstDataModel,
    TransferStatusEnum,
    GraphTransferTaskModel
)
from apps.params import SearchResp, SearchTeachersPageData, PaginationResp, GetTransferHistoryData
from apps.permission.models import Users, UserRole
from common.exceptions import ApiException
from common.security import get_md5_password, get_password_hash
from core import constants
from db.db_mysql import mysql_transaction
from db.base_curd import BaseCurd
from db.db_neo4j.neo4j_const import N4jConst
from db.db_neo4j.schemas import NodeLinkSchema


class CustomQuery(Query):

    def _graph__release_admin_data_right_clause(self, criterion: list[BinaryExpression]):
        """
        这个语句主要是用来给admin用户开放数据权限的。
        用法是：
            1：先找到graph表中user_id那一条语句，如果判断用户ID是admin的ID，就删掉语句
            2：然后把"user_id is not null"加入到where语句中，这样就等同开放了admin的数据权限，即admin可看到所有用户数据
        """
        for i, clause in enumerate(criterion):
            try:
                if clause.left.table.name == GraphModel.__tablename__:
                    # 找到 user_id 那条sql，且等号右边是admin的用户ID
                    if clause.left.description == 'user_id':
                        if str(clause.right.value) == str(constants.ADMIN_USER_ID):
                            criterion.append(GraphModel.user_id.is_not(None))
                            criterion.pop(i)
                            break
            except Exception as _:
                # clause的属性不一定都能像上面的代码那样拿到，因此只要有问题就跳过
                continue

    def filter(self, *criterion) -> Query:
        """重写sqlalchemy的filter方法，在每次执行之前做一些数据过滤的操作"""
        criterion = list(criterion) if isinstance(criterion, tuple) else criterion
        self._graph__release_admin_data_right_clause(criterion)
        return super(CustomQuery, self).filter(*criterion)


class MysqlCurd(BaseCurd):

    def __init__(self, model, db: Session):
        super(MysqlCurd, self).__init__(model, db)

        # 涉及到admin的sql，用这个
        self.query = CustomQuery(GraphModel, self.db)
        # exclude
        self.graph_exclude = ['id', 'is_delete', 'operator_id', 'series_id', 'type', 'is_current']
        self.graph_task_exclude = ['is_delete', 'operator_id', 'task_data']
        self.graph_files_exclude = ['is_delete', 'operator_id', 'is_recognise', 'is_textbook']

    def create_graph(self, data: dict) -> dict:
        """创建图谱记录"""
        obj = self.create(data)
        return obj.to_dict(exclude=self.graph_exclude)

    def get_major_data(self) -> list:
        """获取专业信息，根据parent_id获取各级类目的专业数据"""
        result = list()

        def group_by(obj_list) -> dict:
            """按parent_id，对专业分组"""
            out = dict()
            for obj in obj_list:
                p_id = obj['parent_id']
                if p_id not in out:
                    out[p_id] = list()
                out[p_id].append(obj)
            return out

        # 各级类目数据
        main_objs = self.db.query(
            self.model.id,
            self.model.data_value,
            self.model.parent_id
        ).filter(
            self.model.is_delete == 0,
            self.model.data_label == constants.DATA_LABEL_SUBJECT_CATEGORY,
            self.model.parent_id.is_(None)
        ).order_by(self.model.id).all()

        sub_objs = self.db.query(
            self.model.id,
            self.model.data_value,
            self.model.parent_id
        ).filter(
            self.model.is_delete == 0,
            self.model.data_label == constants.DATA_LABEL_SUBJECT_CATEGORY,
            self.model.data_key == 'sub_category'
        ).order_by(self.model.id).all()
        sub_map = group_by(sub_objs)

        major_objs = self.db.query(
            self.model.id,
            self.model.data_value,
            self.model.parent_id
        ).filter(
            self.model.is_delete == 0,
            self.model.data_label == constants.DATA_LABEL_SUBJECT_CATEGORY,
            self.model.data_key == 'major'
        ).order_by(self.model.id).all()
        major_map = group_by(major_objs)

        # 拼接数据
        for main in main_objs:
            _main = self.model.row_to_dict(main, get_enum='name')
            data = dict(id=_main['id'], name=_main['data_value'], parent_id=_main['parent_id'], children=list())
            for sub in sub_map.get(_main['id'], list()):
                s_data = dict(id=sub['id'], name=sub['data_value'], parent_id=sub['parent_id'], children=list())
                for major in major_map.get(sub['id'], list()):
                    m_data = dict(id=major['id'], name=major['data_value'], parent_id=major['parent_id'], children=None)
                    s_data['children'].append(m_data)
                data['children'].append(s_data)
            result.append(data)

        return result

    def get_overview_data(self, user_id: int) -> dict:
        """
        获取 图谱列表-数据概览 数据
            1. 只能是'正常图谱'类型、且是current的图谱参与数据统计，其他类型不统计
        :param user_id:
        :return:
        """
        result = dict(material_num=0, graph_num=0, handout_num=0, knowledge_num=0, relation_num=0)
        admin_id = constants.ADMIN_USER_ID
        user_clause = 'and user_id is not null' if user_id == admin_id else f'and user_id = {user_id}'

        # current图谱
        # sum()返回的是Decimal类型，需要转成整数
        _type = tuple(GraphTypeEnum.get_statistic_type(get_enum='value'))
        sql = f'''
            select
            count(1) as graph_cnt,
            sum(material_cnt) as material_cnt,
            sum(knowledge_cnt) as knowledge_cnt,
            sum(relation_cnt) as relation_cnt
            from edu_graph
            where is_delete = 0
            and is_current = 1
            {user_clause}
            and type in {_type}
        '''
        sql_out = self.raw_query(sql)
        if sql_out:
            result['material_num'] = int(sql_out[0]['material_cnt'] if sql_out[0]['material_cnt'] else 0)
            result['graph_num'] = int(sql_out[0]['graph_cnt'] if sql_out[0]['graph_cnt'] else 0)
            result['knowledge_num'] = int(sql_out[0]['knowledge_cnt'] if sql_out[0]['knowledge_cnt'] else 0)
            result['relation_num'] = int(sql_out[0]['relation_cnt'] if sql_out[0]['relation_cnt'] else 0)

        # 统计讲义数量
        sql = f'''
            select 
            count(1) as cnt 
            from edu_graph as a
            inner join (
                select max(id) as id from edu_graph
                where is_delete = 0
                and is_publish = 1
                and status = "{GraphStatusEnum.running.value}"
                {user_clause}
                group by series_id
            ) as b
            on a.id = b.id
        '''
        sql_out = self.raw_query(sql)
        if sql_out:
            result['handout_num'] = sql_out[0]['cnt']

        return result

    def search_graphs(
            self,
            user_id: int,
            keyword: Optional[str],
            status: Optional[str],
            start_time: Optional[datetime.datetime],
            end_time: Optional[datetime.datetime],
            page: int,
            page_size: int
    ) -> dict:
        """图谱搜索"""
        result = dict(graphs=list(), total=0, page=page, page_size=page_size)

        sql_out = self.query.filter(
            self.model.is_delete == 0,
            self.model.user_id == user_id,
            self.model.is_current == 1
        )

        if keyword:
            filters = [
                self.model.uuid == keyword,
                self.model.name.contains(keyword)
            ]
            # 这里的条件都是or的关系
            sql_out = sql_out.filter(or_(*filters))

        if status:
            sql_out = sql_out.filter(self.model.status == getattr(GraphStatusEnum, status))
        if start_time:
            sql_out = sql_out.filter(self.model.gmt_create >= start_time)
        if end_time:
            sql_out = sql_out.filter(self.model.gmt_create <= end_time)

        result['total'] = sql_out.count()
        sql_out = sql_out.order_by(self.model.id.desc())
        sql_out = sql_out.limit(page_size).offset((page - 1) * page_size).all()
        graphs = [obj.to_dict(get_enum='name', exclude=self.graph_exclude) for obj in sql_out]

        # 根据查询结果，增加专业id对应的专业名称
        major_ids = [g['major_id'] for g in graphs]
        major_obj = self.db.query(ConstDataModel).filter(
            ConstDataModel.is_delete == 0,
            ConstDataModel.data_label == constants.DATA_LABEL_SUBJECT_CATEGORY,
            ConstDataModel.id.in_(major_ids)
        ).all()
        major_map = {obj.id: obj.data_value for obj in major_obj}
        for g in graphs:
            g['major_name'] = major_map.get(g['major_id'])
        # 根据查询结果，增加用户名称
        user_ids = [g['user_id'] for g in graphs]
        user_obj = self.db.query(Users).filter(
            Users.is_delete == 0,
            Users.id.in_(user_ids)
        ).all()
        user_map = {obj.id: obj.username for obj in user_obj}
        for g in graphs:
            g['user_name'] = user_map.get(g['user_id'])

        result['graphs'] = graphs
        return result

    def delete_graph(self, graph_uuid: str, user_id: int):
        """删除图谱"""
        graph_obj = self.query.filter(
            self.model.is_delete == 0,
            self.model.user_id == user_id,
            self.model.uuid == graph_uuid
        )
        assert graph_obj.count() > 0, '图谱不存在'

        # 若图谱是忙碌态，就不允许被删除
        busy_graph = self.query.filter(
            self.model.is_delete == 0,
            self.model.uuid == graph_uuid,
            self.model.user_id == user_id,
            self.model.status.in_(GraphStatusEnum.get_busy_status())
        )
        assert busy_graph.count() == 0, '图谱有任务正在运行，不可被删除，请前往任务列表查看'

        self.update(graph_obj, dict(is_delete=True, operator_id=user_id))

    def edit_graph_info(self, graph_uuid: str, user_id: int, data: dict):
        """编辑图谱信息"""
        graph_obj = self.query.filter(
            self.model.is_delete == 0,
            self.model.user_id == user_id,
            self.model.uuid == graph_uuid,
            self.model.is_current == 1
        )
        graph_cnt = graph_obj.count()
        assert graph_cnt > 0, f'图谱不存在'
        assert graph_cnt == 1, f'图谱存在多个，ID={graph_uuid}'

        self.update(graph_obj, data)
        graph_obj = graph_obj.first()
        return graph_obj.to_dict(exclude=self.graph_exclude)

    def is_current_graph_exist(self, graph_uuid: str, user_id: int) -> bool:
        """判断图谱是否存在，且图谱 is_current=1 """
        graph_obj = self.query.filter(
            GraphModel.is_delete == 0,
            GraphModel.user_id == user_id,
            GraphModel.uuid == graph_uuid,
            GraphModel.is_current == 1
        )
        return graph_obj.count() == 1

    def is_current_graph_running(self, graph_uuid: str, user_id: int) -> bool:
        """判断用户有图谱权限，且图谱为current图谱、状态为running"""
        graph_obj = self.query.filter(
            GraphModel.is_delete == 0,
            GraphModel.user_id == user_id,
            GraphModel.uuid == graph_uuid,
            GraphModel.is_current == 1,
            GraphModel.status == GraphStatusEnum.running.value
        )
        return graph_obj.count() == 1

    def is_graph_task_exist(
            self,
            graph_uuid: str,
            task_type: Union[str, list],
            *,
            task_status: Union[str, list] = None
    ) -> bool:
        """
        判断图谱的任务是否存在
        :param graph_uuid: <str>
        :param task_type: <str, list> 任务类型值
        :param task_status: <str, list> 任务状态
        :return:
        """
        task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.graph_uuid == graph_uuid,
        )
        # 任务类型过滤
        if isinstance(task_type, str):
            task_obj = task_obj.filter(GraphTaskModel.type == task_type)
        elif isinstance(task_type, list):
            task_obj = task_obj.filter(GraphTaskModel.type.in_(task_type))
        else:
            raise AssertionError('任务状态类型异常')
        # 任务状态过滤
        if isinstance(task_status, str):
            task_obj = task_obj.filter(GraphTaskModel.status == task_status)
        elif isinstance(task_status, list):
            task_obj = task_obj.filter(GraphTaskModel.status.in_(task_status))

        return task_obj.count() > 0

    def get_graph_update_info(self, graph_uuid: str) -> dict:
        """获取图谱的文件、是否更新等数据"""
        result = dict(auto_update=False, files=list())

        # 文件数据
        file_objs = self.db.query(GraphFilesModel).filter(
            GraphFilesModel.is_delete == 0,
            GraphFilesModel.graph_uuid == graph_uuid
        ).all()
        for obj in file_objs:
            result['files'].append(dict(
                file_name=obj.name,
                file_type=obj.file_type.name,
                file_source=obj.file_source.name
            ))

        # 是否定时更新数据
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.uuid == graph_uuid,
        ).first()
        if graph_obj.is_auto_update:
            result['auto_update'] = True

        return result

    def search_tasks(
            self,
            *,
            user_id: int,
            keyword: Optional[str],
            task_status: Optional[str],
            task_type: Optional[str],
            start_time: Optional[datetime.datetime],
            end_time: Optional[datetime.datetime],
            page: int,
            page_size: int
    ) -> dict:
        """任务搜索"""
        result = dict(tasks=list(), total=0, page=page, page_size=page_size)
        # 处理一下入参
        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_time, datetime.datetime) else None
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_time, datetime.datetime) else None
        task_status = getattr(GraphTaskStatusEnum, task_status).value if task_status else None
        task_type = getattr(GraphTaskTypeEnum, task_type).value if task_type else None

        # 根据 keyword 值，准备kw_sql
        kw_sql = ''
        if keyword:
            kw_sql = 'and ('
            kw_sql = f'{kw_sql} name like "%{keyword}%"'
            kw_sql = f'{kw_sql} or uuid like "%{keyword}%"'
            kw_sql = f'{kw_sql} )'
        # 根据其他参数，准备条件sql
        fields_sql = 't.id, t.gmt_create, t.type, t.status, t.percent, t.message, g.uuid, g.name'
        cnt_sql = 'count(1) as cnt'
        user_sql = f'and user_id = {user_id}' if user_id != constants.ADMIN_USER_ID else ''
        start_time_sql = f'and t.gmt_create >= "{start_time}"' if start_time else ''
        end_time_sql = f'and t.gmt_create <= "{end_time}"' if end_time else ''
        status_sql = f'and t.status = "{task_status}"' if task_status else ''
        type_sql = f'and t.type = "{task_type}"' if task_type else ''
        order_by_sql = f'order by t.gmt_create desc'
        limit_sql = f'limit {page_size} offset {(page - 1) * page_size}'
        # 拼接基础sql
        sql = f'''
            select
            {{fields_sql}}
            from edu_graph_task as t
            inner join (
                select uuid, name from edu_graph
                where is_current = 1
                {kw_sql}
                {user_sql}
                order by id desc
                limit 9999
            ) as g on t.graph_uuid = g.uuid
            where t.is_delete = 0
            {start_time_sql}
            {end_time_sql}
            {status_sql}
            {type_sql}
            {order_by_sql}
            {{limit_sql}}
        '''

        # 查询
        sql_cnt = sql.format(fields_sql=cnt_sql, limit_sql='')
        sql_out = self.raw_query(sql_cnt)
        result['total'] = sql_out[0]['cnt']
        sql_tasks = sql.format(fields_sql=fields_sql, limit_sql=limit_sql)
        sql_out = self.raw_query(sql_tasks)
        result['tasks'] = sql_out
        # 最后过滤一下结果，把枚举值的value替换成name
        for data in result['tasks']:
            data['type'] = GraphTaskTypeEnum(data['type']).name
            data['status'] = GraphTaskStatusEnum(data['status']).name

        return result

    def is_graph_owner(self, graph_uuid: str, user_id: int) -> bool:
        """判断用户是否为图谱的owner，包含admin的逻辑"""
        graph_obj = self.query.filter(
            GraphModel.uuid == graph_uuid,
            GraphModel.user_id == user_id
        )
        return True if graph_obj.count() == 1 else False

    def graph_build_waiting_info(self, graph_uuid: str, user_id: int):
        """新建图谱-图谱构建等待页数据"""
        result = dict()

        # 验证用户有图谱权限
        assert self.is_graph_owner(graph_uuid, user_id), '无权限查看该图谱'
        # 图谱挂载的任务数据
        task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.graph_uuid == graph_uuid,
            GraphTaskModel.type == GraphTaskTypeEnum.build.value
        ).first()
        assert task_obj is not None, '任务不存在'

        exclude_fields = ['is_delete', 'operator_id', 'message', 'task_data', 'is_clear']
        result.update(task_obj.to_dict(get_enum='name', exclude=exclude_fields))
        return result

    def graph_update_waiting_info(self, graph_uuid: str):
        """图谱更新等待页，返回任务数据"""
        # 图谱挂载的任务数据
        task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.graph_uuid == graph_uuid,
            GraphTaskModel.type == GraphTaskTypeEnum.update.value
        ).order_by(GraphTaskModel.id.desc()).first()
        assert task_obj is not None, '任务不存在'

        exclude_fields = ['is_delete', 'operator_id', 'message', 'task_data', 'is_clear']
        return task_obj.to_dict(get_enum='name', exclude=exclude_fields)

    def is_task_owner(self, task_id: int, user_id: int) -> bool:
        """判断是否为任务的owner"""
        task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.id == task_id
        ).first()
        if not task_obj:
            return False
        # 通过判断用户是否拥有图谱权限，来判断用户是否拥有任务权限
        return self.is_graph_owner(task_obj.graph_uuid, user_id)

    def is_valid_major(self, major_id: int) -> bool:
        """创建图谱时，验证传进来的学科分类ID是正确的"""
        major_obj = self.db.query(ConstDataModel).get(major_id)
        return major_obj is not None

    def task_cancel(self, task_id: int, user_id: int):
        """取消任务"""
        task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.status.in_(GraphTaskStatusEnum.task_unfinished_status(get_enum='value')),
            GraphTaskModel.id == task_id
        ).first()
        # 先确认当前任务的状态是可取消的
        assert task_obj is not None, '该状态下任务无法取消'

        # 取消当前任务
        self.db.query(GraphTaskModel).filter(
            GraphTaskModel.id == task_id
        ).update(dict(
            status=GraphTaskStatusEnum.cancel.value,
            operator_id=user_id,
            message='cancel'
        ))
        self.db.commit()

    def is_task_able_to_retry(self, task_id: int, user_id: int) -> bool:
        """判断当前任务是否可重试"""
        # 失败的任务才可重试
        task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.status == GraphTaskStatusEnum.fail.value,
            GraphTaskModel.id == task_id
        ).first()
        assert task_obj is not None, '该状态下任务不可重试'
        # 通过判断用户是否拥有图谱权限，来判断用户是否拥有任务权限
        assert self.is_graph_owner(task_obj.graph_uuid, user_id), '无数据权限'
        return True

    def task_retry(
            self,
            *,
            task_id: int,
            user_id: int,
            graph_uuid: str,
            task_type: str
    ):
        """
        重试任务，更新任务、图谱状态
        :param task_id: <int>
        :param user_id: <int>
        :param graph_uuid: <str> 图谱UUID
        :param task_type: <str> 任务类型，根据这个更新图谱状态
        :return:
        """
        # 获取图谱状态
        if task_type == GraphTaskTypeEnum.build.value:
            graph_status = GraphStatusEnum.building.value
        elif task_type == GraphTaskTypeEnum.merge.value:
            graph_status = GraphStatusEnum.merging.value
        elif task_type == GraphTaskTypeEnum.update.value:
            graph_status = GraphStatusEnum.updating.value
        elif task_type == GraphTaskTypeEnum.publish.value:
            graph_status = GraphStatusEnum.publishing.value
        elif task_type == GraphTaskTypeEnum.cron_graph_update.value:
            graph_status = GraphStatusEnum.updating.value
        else:
            graph_status = GraphStatusEnum.fail.value

        with mysql_transaction(self.db):
            # 更新任务状态
            self.db.query(GraphTaskModel).filter(
                GraphTaskModel.is_delete == 0,
                GraphTaskModel.id == task_id
            ).update(dict(
                operator_id=user_id,
                status=GraphTaskStatusEnum.running.value,
                message='retrying ...'
            ))
            # 更新图状态
            self.db.query(GraphModel).filter(
                GraphModel.is_delete == 0,
                GraphModel.uuid == graph_uuid
            ).update(dict(
                operator_id=user_id,
                status=graph_status
            ))
            self.db.commit()

    def is_have_graph_access(
            self,
            graph_uuid: str,
            user_id: int,
            captcha_list: Optional[list] = None
    ) -> bool:
        """
        判断用户是否有图谱的访问权限
        :param graph_uuid: <str> 图谱UUID
        :param user_id: <int> 用户ID
        :param captcha_list: <list> 验证码。只有普通用户（学生）账号才会携带这个参数，通过参数判断是否有图谱的访问权限
        :return:
        """
        # 先通过用户ID判断是否是图谱的owner
        have_access = self.is_graph_owner(graph_uuid, user_id)
        # 如果用户不是图谱的owner，判断captcha是否匹配（这里一定是发布的图谱）
        if not have_access:
            if captcha_list:
                obj = self.db.query(GraphModel).filter(
                    GraphModel.is_delete == 0,
                    GraphModel.is_publish == 1,
                    GraphModel.uuid == graph_uuid,
                    GraphModel.captcha.is_not(None),
                    GraphModel.captcha.in_(captcha_list)
                )
                if obj.count() > 0:
                    have_access = True

        return have_access

    def get_graph_gid(self, graph_uuid: str, *, captcha: Optional[list] = None) -> str:
        """
        获取图谱ID（标签）
            1. 获取图谱详情，现在有两套入口：
                1). 一套是教师、admin，正常查看详情；
                2). 另一套是学生、教师、admin，通过验证码获取图谱详情；
            2. 先判断验证码，若非空，直接从 GraphModel 中获取gid
            3. 再通过UUID获取图谱，从 GraphToGraphModel 表中获取gid
            4. 以上两种情况不会产生交集，因此可以做先、后的覆盖处理
        :param graph_uuid: <str> 图谱UUID
        :param captcha: <list> 验证码
        :return: <str> 图谱ID（标签）
        """
        gid = None

        # 先通过验证码获取发布图谱gid
        # 这里不需要把captcha添加到orm的查询条件中，已经在别的地方做了captcha的判断了
        if captcha:
            publish_graph_obj = self.db.query(GraphModel).filter(
                GraphModel.is_delete == 0,
                GraphModel.is_publish == 1,
                GraphModel.uuid == graph_uuid
            ).first()
            if publish_graph_obj:
                gid = str(publish_graph_obj.id)
                return gid

        # 若前面没获取到gid，说明查看的是current图谱，再通过UUID获取current图谱gid
        g2g_obj = self.db.query(GraphToGraphModel).filter(
            GraphToGraphModel.is_delete == 0,
            GraphToGraphModel.graph_uuid == graph_uuid
        ).first()
        if g2g_obj:
            gid = str(g2g_obj.graph_id)

        assert bool(gid), '未获取到图谱标签'
        return gid

    def get_graph_name(self, graph_uuid: str) -> str:
        """
        获取图谱名称
        :param graph_uuid:
        :return: <str> 图谱名称
        """
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.uuid == graph_uuid
        ).first()
        return graph_obj.name

    def get_graph_uuid(self, gid: int) -> str:
        """获取图谱的uuid"""
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.id == gid
        ).first()
        return graph_obj.uuid

    def get_knowledge_source_in_node_creation(self, graph_uuid: str) -> list[dict]:
        """新增知识点-知识点来源。返回当前图谱的所有教材"""
        result = list()

        file_objs = self.db.query(GraphFilesModel).filter(
            GraphFilesModel.is_delete == 0,
            GraphFilesModel.graph_uuid == graph_uuid,
            GraphFilesModel.is_recognise == 1
        ).all()
        for obj in file_objs:
            result.append(dict(
                id=obj.id,
                name=obj.name,
                graph_uuid=obj.graph_uuid,
                type=obj.file_source.name,
                link=obj.file_url or obj.relative_path
            ))

        result.sort(key=lambda x: x['name'])
        return result

    def is_graph_running(self, graph_uuid) -> bool:
        """
        判断current图谱的状态是否为运行中
        :param graph_uuid:
        :return: <bool>
        """
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.is_current == 1,
            GraphModel.uuid == graph_uuid,
            GraphModel.status == GraphStatusEnum.running.value
        )
        return graph_obj.count() > 0

    def is_graph_able_to_build(self, graph_uuid: str, user_id: int) -> bool:
        """
        判断图谱是否可构建
            1. 图谱是current图谱，且未删除
            2. 图谱是normal类型图谱，且为'就绪'状态
        :param graph_uuid:
        :param user_id:
        :return:
        """
        # 图谱是current图谱，且未删除
        assert self.is_current_graph_exist(graph_uuid, user_id), '图谱不存在，无法构建图谱'
        # 图谱是normal类型图谱，且为'就绪'状态
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.uuid == graph_uuid,
            GraphModel.type == GraphTypeEnum.normal.value,
            GraphModel.status == GraphStatusEnum.ready.value
        ).first()
        return graph_obj is not None

    def is_graph_able_to_publish(self, graph_uuid: str) -> bool:
        """判断当前图谱是否能够发布"""
        # 判断图谱状态是运行中
        assert self.is_graph_running(graph_uuid), '图谱发布中'
        # 判断发布任务是否已存在
        publish_task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.graph_uuid == graph_uuid,
            GraphTaskModel.type == GraphTaskTypeEnum.publish.value,
            GraphTaskModel.status.in_(GraphTaskStatusEnum.task_unfinished_status(get_enum='value'))
        )
        assert publish_task_obj.count() == 0, '尚有发布任务未完成'
        return True

    def create_publish_task_and_graph(self, current_graph_uuid: str, user_id: int) -> tuple:
        """
        创建发布任务和发布图谱
        :param current_graph_uuid: <str> current图谱的UUID
        :param user_id: <int> 发布者ID
        :return: <tuple>
            publish_graph_uuid: <str> 发布图谱的UUID，
            task_id: <int> 发布任务ID
        """
        with mysql_transaction(self.db):
            current_graph_obj = self.db.query(GraphModel).filter(GraphModel.uuid == current_graph_uuid).first()
            # 创建发布任务
            task_obj = GraphTaskModel(
                operator_id=user_id,
                graph_uuid=current_graph_uuid,
                type=GraphTaskTypeEnum.publish.value,
                status=GraphTaskStatusEnum.running.value,
                message='running ...'
            )
            self.db.add(task_obj)
            # 创建发布图谱(验证码在后台任务中生成)
            publish_graph_uuid = GraphModel.generate_uuid()
            publish_graph_obj = GraphModel(
                operator_id=user_id,
                uuid=publish_graph_uuid,
                name=current_graph_obj.name,
                series_id=current_graph_obj.series_id,
                user_id=current_graph_obj.user_id,
                status=GraphStatusEnum.publishing.value,
                type=GraphTypeEnum.normal.value,
                is_current=False,
                is_publish=True,
                major_id=current_graph_obj.major_id,
                graph_type=current_graph_obj.graph_type,
                graph_desc=current_graph_obj.graph_desc,
                material_cnt=current_graph_obj.material_cnt,
                knowledge_cnt=current_graph_obj.knowledge_cnt,
                relation_cnt=current_graph_obj.relation_cnt
            )
            self.db.add(publish_graph_obj)
            # 更新current图谱状态
            self.db.query(GraphModel).filter(
                GraphModel.is_delete == 0,
                GraphModel.uuid == current_graph_uuid
            ).update(dict(
                operator_id=user_id,
                status=GraphStatusEnum.publishing.value
            ))
            self.db.commit()

        # 组织任务数据，其他地方会用到
        _task_data = dict(gid=str(publish_graph_obj.id), publish_graph_uuid=publish_graph_uuid)
        self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.id == task_obj.id
        ).update(dict(task_data=json.dumps(_task_data, ensure_ascii=False)))
        self.db.commit()

        return publish_graph_uuid, task_obj.id

    def search_publish_graph(
            self,
            user_id: int,
            keyword: Optional[str],
            start_time: Optional[datetime.datetime],
            end_time: Optional[datetime.datetime],
            page: int,
            page_size: int
    ) -> dict:
        """搜索发布的图谱"""
        admin_id = constants.ADMIN_USER_ID
        result = dict(graphs=list(), total=0, page=page, page_size=page_size)

        # 根据入参组织查询语句
        kw_clause = ''
        if keyword:
            kw_clause = f'name like "%{keyword}%"'
            kw_clause = f'{kw_clause} or uuid = "{keyword}"'
            kw_clause = f'and ({kw_clause})'
        # 根据其他参数，准备条件sql
        user_clause = 'and user_id is not null' if user_id == admin_id else f'and user_id = {user_id}'
        status_clause = f'and status = "{GraphStatusEnum.running.value}"'
        limit_clause = f'limit {page_size} offset {(page - 1) * page_size}'
        start_time_clause = f'and gmt_create >= "{start_time}"' if start_time else ''
        end_time_clause = f'and gmt_create <= "{end_time}"' if end_time else ''
        order_by_clause = f'order by a.gmt_create desc'
        # 统计不同数据，需要不同的字段
        data_fields = 'a.gmt_create, a.gmt_modified, a.uuid, a.name'
        data_fields = f'{data_fields}, a.material_cnt, a.knowledge_cnt, a.relation_cnt, a.captcha'
        cnt_fields = 'count(1) as cnt'

        # 基础sql
        sql = f'''
            select 
            {{fields}} 
            from edu_graph as a
            inner join (
                select max(id) as id from edu_graph
                where is_delete = 0
                and is_publish = 1
                {status_clause}
                {kw_clause}
                {user_clause}
                {start_time_clause}
                {end_time_clause}
                group by series_id
            ) as b
            on a.id = b.id
            {order_by_clause}
            {{limit_clause}}
        '''

        # 统计总量
        cnt_sql = sql.format(fields=cnt_fields, limit_clause='')
        sql_out = self.raw_query(cnt_sql)
        result['total'] = sql_out[0]['cnt']

        # 准备用户数据，要拼接到返回值中
        user_obj = self.db.query(Users).filter(
            Users.is_delete == 0,
            Users.id == user_id
        ).first()

        # 组织返回数据
        data_sql = sql.format(fields=data_fields, limit_clause=limit_clause)
        sql_out = self.raw_query(data_sql)
        result['graphs'] = sql_out
        # 添加用户名、版本号
        for _data in result['graphs']:
            _data['user'] = user_obj.nickname
            version = _data['gmt_modified'].replace('-', '').replace(':', '').replace(' ', '')
            _data['version'] = f'V{version}'

        return result

    def search_publish_graph_by_captcha(self, captcha: str) -> str:
        """通过验证码搜索发布的图谱，返回图谱UUID"""
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.is_publish == 1,
            GraphModel.captcha == captcha
        ).first()
        assert graph_obj, '图谱不存在'
        return graph_obj.uuid

    def graph_compare_graph_data(self, graph1_uuid: str, graph2_uuid: str) -> dict:
        """图谱对比-图谱数据"""
        # 查询图谱
        graph1_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.is_current == 1,
            GraphModel.uuid == graph1_uuid
        ).first()
        assert graph1_obj is not None, '图谱1不存在'
        graph2_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.is_current == 1,
            GraphModel.uuid == graph2_uuid
        ).first()
        assert graph2_obj is not None, '图谱2不存在'

        # 学科数据
        major_objs = self.db.query(ConstDataModel).filter(
            ConstDataModel.is_delete == 0,
            ConstDataModel.data_label == constants.DATA_LABEL_SUBJECT_CATEGORY,
            ConstDataModel.id.in_([graph1_obj.major_id, graph2_obj.major_id])
        ).all()
        major_map = {_obj.id: _obj.data_value for _obj in major_objs}

        # 返回数据
        _exclude = self.graph_exclude + ['is_publish', 'major_id', 'captcha']
        result = {
            graph1_uuid: graph1_obj.to_dict(get_enum='name', exclude=_exclude),
            graph2_uuid: graph2_obj.to_dict(get_enum='name', exclude=_exclude)
        }
        result[graph1_uuid]['major'] = major_map.get(graph1_obj.major_id)
        result[graph2_uuid]['major'] = major_map.get(graph2_obj.major_id)

        return result

    def fetch_node_text(self, content_id: int) -> dict:
        """
        返回知识点的文本内容，和教学材料的相对路径
        :param content_id: <int> 文本id
        :return:
        """
        result = dict(
            file=dict(name='', link=''),
            content=dict(id=content_id, type='', content='')
        )

        # 知识点内容
        content_obj = self.db.query(NodeTextModel).filter(
            NodeTextModel.is_delete == 0,
            NodeTextModel.id == content_id
        ).first()
        assert content_obj, '未获取到知识点内容'
        result['content']['type'] = content_obj.content_type.name
        result['content']['content'] = content_obj.node_text

        # 文件相对路径
        file_obj = self.db.query(GraphFilesModel).filter(
            GraphFilesModel.is_delete == 0,
            GraphFilesModel.id == content_obj.file_id
        ).first()
        if file_obj:
            result['file']['link'] = file_obj.relative_path
            result['file']['name'] = file_obj.name

        return result

    def modify_node_text(self, text_id: int, text_content: str) -> dict:
        """
        修改知识点内容
        :param text_id: <int> 文本id
        :param text_content: <str> 修改后的文本内容
        :return:
        """
        result = dict(
            file=dict(name='', link=''),
            content=dict(id=text_id, type='', content=text_content)
        )

        # 知识点内容
        text_obj = self.db.query(NodeTextModel).filter(
            NodeTextModel.is_delete == 0,
            NodeTextModel.id == text_id
        ).first()
        assert text_obj, '未获取到知识点内容'
        result['content']['type'] = text_obj.content_type.name

        # 更新知识点内容
        self.db.query(NodeTextModel).filter(
            NodeTextModel.is_delete == 0,
            NodeTextModel.id == text_id
        ).update(dict(node_text=text_content))
        self.db.commit()

        # 文件相对路径
        file_obj = self.db.query(GraphFilesModel).filter(
            GraphFilesModel.is_delete == 0,
            GraphFilesModel.id == text_obj.file_id
        ).first()
        if file_obj:
            result['file']['link'] = file_obj.relative_path
            result['file']['name'] = file_obj.name

        return result

    def __new_text_prop(self, id_content_map: dict[int, NodeTextModel], text_ids: list[int]) -> list:
        """
        返回"知识点内容"的值
        :param id_content_map: ID与内容的映射表
        :param text_ids: 知识内容的主键ID
        :return:
            [
                {type: xx, content: xxx}
            ]
        """
        out = list()

        for _id in text_ids:
            _obj = id_content_map.get(_id, None)
            if _obj:
                out.append(dict(
                    type=_obj.content_type.name,
                    content=_obj.node_text
                ))

        return out

    def change_text_id_to_node_text(self, nodes_data: Union[list, dict]):
        """
        将节点数据中的"知识点内容"进行转换，文本ID转换成文本内容
        :param nodes_data: <list> 节点数据
            e.g. [{
                    'label': [...],
                    'prop': {'知识点内容': [...], ...}
                }, ... ]
        :return:
        """
        all_text_id = list()
        id_content_map = dict()

        if isinstance(nodes_data, list):
            for _node in nodes_data:
                if 'prop' in _node and constants.NodePropName.KWL_TEXT in _node['prop']:
                    all_text_id.extend(_node['prop'][constants.NodePropName.KWL_TEXT])
            all_text_id = list(set(all_text_id))
            # 为了减少数据库的查询次数，这里一次获取到所有的文本ID（前提是每个知识点最多6个文本ID，最多100个知识点）
            # 生成一个 id：text 的映射表，然后逐个替换节点数据，这样效率较高
            text_objs = self.db.query(NodeTextModel).filter(
                NodeTextModel.is_delete == 0,
                NodeTextModel.id.in_(all_text_id)
            ).all()
            for _obj in text_objs:
                id_content_map[_obj.id] = _obj
            # 替换节点的"知识点内容"数据
            for _node in nodes_data:
                if 'prop' in _node and constants.NodePropName.KWL_TEXT in _node['prop']:
                    _text_ids = _node['prop'][constants.NodePropName.KWL_TEXT]
                    _node['prop'][constants.NodePropName.KWL_TEXT] = self.__new_text_prop(id_content_map, _text_ids)

        elif isinstance(nodes_data, dict):
            if constants.NodePropName.KWL_TEXT in nodes_data:
                all_text_id = nodes_data[constants.NodePropName.KWL_TEXT]
                # 生成一个 id：text 的映射表
                text_objs = self.db.query(NodeTextModel).filter(
                    NodeTextModel.is_delete == 0,
                    NodeTextModel.id.in_(all_text_id)
                ).all()
                for _obj in text_objs:
                    id_content_map[_obj.id] = _obj
                # 替换节点的"知识点内容"数据
                nodes_data[constants.NodePropName.KWL_TEXT] = self.__new_text_prop(id_content_map, all_text_id)

    def complete_material_name(self, nodes_data: Union[list, dict]):
        """
        neo4j中只存储了教材的相对路径，需要根据相对路径，把文件的原始名称找出来，拼到返回里

        :param nodes_data: 节点数据
            e.g. [{
                    'label': [...],
                    'prop': {'知识点内容': [...], ...}
                }, ... ]
        :return:
        """
        all_link = set()
        link_map = dict()

        if isinstance(nodes_data, list):
            for node in nodes_data:
                prop = node['prop']
                if N4jConst.NPName.LINK in prop:
                    for data in prop[N4jConst.NPName.LINK]:
                        _link = data.get('link')
                        if _link:
                            all_link.add(_link)
            # 查出文件对应的文件名
            file_objs = self.db.query(GraphFilesModel).filter(
                GraphFilesModel.is_delete == 0,
                GraphFilesModel.is_recognise == 1,
                GraphFilesModel.relative_path.in_(list(all_link))
            ).all()
            for obj in file_objs:
                link_map[obj.relative_path] = obj.name
            # 先把映射表中已有的文件名补全上
            # 再把映射表中没有的文件名补全上
            for node in nodes_data:
                prop = node['prop']
                if N4jConst.NPName.LINK in prop:
                    for data in prop[N4jConst.NPName.LINK]:
                        _link = data.get('link')
                        if _link:
                            _name = link_map.get(_link)
                            if _name:
                                data['name'] = _name
                    NodeLinkSchema.add_default_name(prop[N4jConst.NPName.LINK])

        elif isinstance(nodes_data, dict):
            prop = nodes_data['prop']
            if N4jConst.NPName.LINK in prop:
                for data in prop[N4jConst.NPName.LINK]:
                    _link = data.get('link')
                    if _link:
                        all_link.add(_link)
                # 查出文件对应的文件名
                file_objs = self.db.query(GraphFilesModel).filter(
                    GraphFilesModel.is_delete == 0,
                    GraphFilesModel.is_recognise == 1,
                    GraphFilesModel.relative_path.in_(list(all_link))
                ).all()
                for obj in file_objs:
                    link_map[obj.relative_path] = obj.name
                # 先把映射表中已有的文件名补全上
                # 再把映射表中没有的文件名补全上
                for data in prop[N4jConst.NPName.LINK]:
                    _link = data.get('link')
                    if _link:
                        _name = link_map.get(_link)
                        if _name:
                            data['name'] = _name
                NodeLinkSchema.add_default_name(prop[N4jConst.NPName.LINK])


class GeneralCurd:
    """mysql 的一些通用的查询"""

    def __init__(self, mysql_client: Session):
        self.db = mysql_client

    def is_graph_owner(self, *, graph_uuid: str, user_id: int) -> bool:
        """是否为图谱拥有者，包含admin"""
        if user_id == constants.ADMIN_USER_ID:
            return True

        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.uuid == graph_uuid,
            GraphModel.user_id == user_id
        ).first()

        return bool(graph_obj)

    def is_graph_ok(self, graph_uuid: str) -> bool:
        """判断图谱的状态是好的，可以查看、编辑、或发起任务"""
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.uuid == graph_uuid
        ).first()
        return bool(graph_obj)

    def update_graph_info(self, *, graph_uuid: str, data: dict):
        """更新图谱记录"""
        self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.uuid == graph_uuid
        ).update(data)
        self.db.commit()


class NewMysqlCurd(GeneralCurd):

    def __init__(self, mysql_client: Session):
        super(NewMysqlCurd, self).__init__(mysql_client)

    def create_graph_import_task(
            self,
            *,
            graph_uuid: str,
            user_id: int,
            excel_name: str
    ) -> dict:
        """
        创建图谱上传任务
        :param graph_uuid: 图谱UUID
        :param user_id:
        :param excel_name: excel文件名
        :return:
        """
        task_data = TaskDataSchema(graph_import=GraphImportSchema(object_name=excel_name))
        task_obj = GraphTaskModel(
            operator_id=user_id,
            graph_uuid=graph_uuid,
            type=GraphTaskTypeEnum.upload.value,
            status=GraphTaskStatusEnum.ready.value,
            percent=0,
            task_data=json.dumps(task_data.dict(), ensure_ascii=False),
            message='start',
        )
        self.db.add(task_obj)
        self.db.commit()
        self.db.refresh(task_obj)

        return task_obj.to_dict(get_enum='name')

    def is_graph_capable_to_import(self, graph_uuid: str) -> bool:
        """图谱当前状态是否可以导入。就绪、运行中的状态可导入"""
        # 未完成的导入任务
        task_obj = self.db.query(GraphTaskModel).filter(
            GraphTaskModel.is_delete == 0,
            GraphTaskModel.graph_uuid == graph_uuid,
            GraphTaskModel.type == GraphTaskTypeEnum.upload.value,
            GraphTaskModel.status.in_(GraphTaskStatusEnum.task_unfinished_status(get_enum='value'))
        ).first()
        if task_obj:
            return False

        # 图谱状态
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.is_current == 1,
            GraphModel.uuid == graph_uuid,
            or_(
                GraphModel.status == GraphStatusEnum.running.value,
                GraphModel.status == GraphStatusEnum.ready.value
            )
        ).first()
        return bool(graph_obj)

    def search_teachers(self, *, keyword: Optional[str], user_id: int) -> dict:
        """查询教师"""
        result = SearchResp(total=0, data=list())

        if keyword:
            subq = self.db.query(
                UserRole.user_id,
                UserRole.role_id,
            ).filter(
                UserRole.is_delete == 0,
                UserRole.user_id != user_id,
                or_(
                    UserRole.role_id == constants.EternalRole.TEACHER,
                    UserRole.role_id == constants.EternalRole.ADMIN,
                )
            ).subquery()
            # 子查询
            user_objs = self.db.query(
                Users.id,
                Users.username,
                Users.nickname,
                subq.c.role_id
            ).filter(
                Users.is_delete == 0,
                Users.is_active == 1,
                Users.status == 0,
                or_(
                    Users.username == keyword,
                    Users.nickname.contains(keyword)
                ),
                Users.id == subq.c.user_id
            ).order_by(Users.id.desc()).limit(20).all()

            for obj in user_objs:
                _user = Users.row_to_dict(obj, get_enum='name')
                result.data.append(SearchTeachersPageData(
                    id=_user['id'],
                    username=_user['username'],
                    nickname=_user['nickname'],
                ))

        return result.dict()

    def is_user_able_to_transfer(self, *, owner_id: int, recipient_id: int, graph_uuid: str):
        """
        判断用户有权限转移图谱、接收图谱
        :param owner_id: 图谱原持有人
        :param recipient_id: 图谱接收者
        :param graph_uuid: 图谱UUID
        :return:
        """
        # 图谱操作权限
        assert self.is_graph_owner(graph_uuid=graph_uuid, user_id=owner_id), '无图谱权限'

        # 权限校验
        role_objs = self.db.query(UserRole).filter(
            UserRole.is_delete == 0,
            or_(
                UserRole.role_id.in_([constants.EternalRole.TEACHER, constants.EternalRole.ADMIN]),
                UserRole.user_id.in_([owner_id, recipient_id]),
            )
        ).all()
        teacher_ids = [obj.user_id for obj in role_objs]
        assert owner_id in teacher_ids, '无权限转移图谱'
        assert recipient_id in teacher_ids, '对方无权限接收图谱'

    def transfer_current_graph(self, *, owner_id: int, recipient_id: int, graph_uuid: str):
        """
        转让图谱
        :param owner_id: 图谱原持有人
        :param recipient_id: 图谱接收者
        :param graph_uuid: 图谱UUID
        :return:
        """
        # 转让：目前转让的是current图谱，则一次把相同series_id的图谱全部转让给对方
        graph_obj = self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.uuid == graph_uuid
        ).first()

        self.db.query(GraphModel).filter(
            GraphModel.is_delete == 0,
            GraphModel.series_id == graph_obj.series_id,
        ).update(dict(user_id=recipient_id))

        # 新增转让记录
        self.db.add(GraphTransferTaskModel(
            owner_id=owner_id,
            recipient_id=recipient_id,
            graph_uuid=graph_uuid,
            status=TransferStatusEnum.success.value,
        ))

        self.db.commit()

    def delete_graph_transfer_entry(self, graph_uuid: str):
        """删除转让记录"""
        self.db.query(GraphTransferTaskModel).filter(
            GraphTransferTaskModel.graph_uuid == graph_uuid
        ).delete()
        self.db.commit()

    def search_transfer_history(
            self, *,
            user_id: int,
            page: int,
            page_size: int,
            graph_name: Optional[str] = None
    ) -> dict:
        """获取转让记录"""
        result = PaginationResp(total=0, data=list(), page=page, page_size=page_size)

        # 教师或者admin才会有记录
        teacher_obj = self.db.query(UserRole).filter(
            UserRole.is_delete == 0,
            UserRole.user_id == user_id,
            or_(
                UserRole.role_id == constants.EternalRole.TEACHER,
                UserRole.role_id == constants.EternalRole.ADMIN,
            )
        ).first()

        if teacher_obj:
            history_objs = self.db.query(GraphTransferTaskModel).filter(GraphTransferTaskModel.is_delete == 0)

            # 数据权限（admin看所有，教师看自己）
            if user_id != constants.ADMIN_USER_ID:
                history_objs = history_objs.filter(or_(
                    GraphTransferTaskModel.owner_id == user_id,
                    GraphTransferTaskModel.recipient_id == user_id
                ))

            # 搜索项
            if graph_name:
                _graph_objs = self.db.query(GraphModel).filter(
                    GraphModel.is_current == 1,
                    GraphModel.type == GraphTypeEnum.normal.value,
                    GraphModel.name.contains(graph_name)
                ).order_by(GraphModel.id.desc()).limit(2000)  # 这里加个数量限制，防止数据过多内存过载
                _search_graph_uuids = [obj.uuid for obj in _graph_objs]
                history_objs = history_objs.filter(GraphTransferTaskModel.graph_uuid.in_(_search_graph_uuids))

            # 总数
            result.total = history_objs.count()
            history_objs = history_objs.order_by(GraphTransferTaskModel.id.desc())
            history_objs = history_objs.limit(page_size).offset((page - 1) * page_size).all()

            # 获取用户数据
            all_user_ids = [obj.owner_id for obj in history_objs]
            all_user_ids.extend([obj.recipient_id for obj in history_objs])
            all_user_ids = list(set(all_user_ids))
            user_objs = self.db.query(Users).filter(
                Users.is_delete == 0,
                Users.id.in_(all_user_ids)
            ).all()
            user_map = {obj.id: obj.nickname for obj in user_objs}

            # 获取图谱数据
            all_graph_uuids = [obj.graph_uuid for obj in history_objs]
            all_graph_uuids = list(set(all_graph_uuids))
            graph_objs = self.db.query(GraphModel).filter(
                GraphModel.is_delete == 0,
                GraphModel.uuid.in_(all_graph_uuids)
            ).all()
            graph_map = {obj.uuid: obj.name for obj in graph_objs}

            # 组装分页数据
            for obj in history_objs:
                _data = obj.to_dict(get_enum='name')
                result.data.append(GetTransferHistoryData(
                    owner_name=user_map[_data['owner_id']],
                    recipient_name=user_map[_data['recipient_id']],
                    graph_uuid=obj.graph_uuid,
                    graph_name=graph_map[_data['graph_uuid']],
                    status=_data['status'],
                    gmt_create=_data['gmt_create'],
                ))

        return result.dict()

    def get_user_by_work_num(self, auth_data: dict) -> dict:
        """用户存在，则通过工号获取用户信息；用户不存在，则创建用户、分配角色"""
        user_obj = self.db.query(Users).filter(Users.username == auth_data['work_num']).first()

        # 因为用户表中的 username 是有唯一索引的，当用户被删除，用相同的username再次创建，提示"账号重复"
        if user_obj and user_obj.is_delete != 0:
            raise ApiException(code=401, msg='账号已存在')

        if not user_obj:
            # 默认密码格式：{work_num}_{code}
            # 多用md5编码一次默认密码，这样用户就不能通过教学系统登录进来了（清华老师要求的）
            md5_pwd = get_md5_password(f'{auth_data["work_num"]}_{auth_data["code"]}')
            md5_pwd_plus = get_md5_password(md5_pwd)
            hash_pwd = get_password_hash(md5_pwd_plus)

            # 创建用户
            user_obj = Users(
                creator_id=constants.ADMIN_USER_ID,
                modifier_id=constants.ADMIN_USER_ID,
                username=auth_data['work_num'],
                nickname=auth_data['name'],
                phone=auth_data.get('phone') or '13123456789',
                email=auth_data.get('email') or 'user@foo.email',
                hashed_password=hash_pwd,
                avatar='avatar.jpg',
                status=0,
                is_active=True,
                is_superuser=False
            )
            self.db.add(user_obj)
            self.db.commit()
            self.db.refresh(user_obj)

            # 分配角色
            self.db.add(UserRole(
                creator_id=constants.ADMIN_USER_ID,
                modifier_id=constants.ADMIN_USER_ID,
                user_id=user_obj.id,
                role_id=Tsinghua.get_user_role(auth_data['user_type']),
            ))
            self.db.commit()

        return user_obj.to_dict(get_enum='name')
