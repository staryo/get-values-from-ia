import json
import ssl
import uuid
from datetime import datetime
from websocket import create_connection
from functools import partialmethod
from mimetypes import guess_type
from time import sleep
from urllib.parse import urljoin

import requests.exceptions
from requests import Session
from tqdm import tqdm

__all__ = [
    'GetDataFromBFG',
]

from base.base import Base
from utils.list_to_dict import list_to_dict

_DATETIME_SIMPLE_FORMAT = '%Y-%m-%dT%H:%M:%S'


class GetDataFromBFG(Base):

    def __init__(self, login, password, base_url, verify, ws_url, time_zone,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = Session()
        self._session.verify = verify
        self._base_url = base_url
        self._ws_url = ws_url
        self._login = login
        self._password = password
        self._time_zone = time_zone

        self.cache = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def _make_url(self, uri):
        return urljoin(self._base_url, uri)

    @staticmethod
    def _make_entity_name(filename, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()
        return '{} ({})'.format(
            filename,
            timestamp.strftime(_DATETIME_SIMPLE_FORMAT),
        )

    def _perform_json_request(self, http_method, uri, **kwargs):
        url = self._make_url(uri)
        logger = self._logger

        logger.debug('Выполнение {} запроса '
                     'по ссылке {!r}.'.format(http_method, url))

        logger.debug('Отправляемые данные: {!r}.'.format(kwargs))

        sleep(0.2)
        try:
            response = self._session.request(http_method,
                                             url=url,
                                             **kwargs).json()
        except requests.exceptions.JSONDecodeError:
            sleep(2)
            response = self._session.request(http_method,
                                             url=url,
                                             **kwargs).json()
        logger.debug('Получен ответ на {} запрос по ссылке {!r}: '
                     '{!r}'.format(http_method, url, response))
        return response

    _perform_get = partialmethod(_perform_json_request, 'GET')

    def _perform_post(self, uri, data):
        return self._perform_json_request('POST', uri, json=data)

    def _perform_put(self, uri, data):
        return self._perform_json_request('PUT', uri, json=data)

    def _perform_delete(self, uri):
        return self._perform_json_request('DELETE', uri)

    def _perform_action(self, uri_part, **data):
        return self._perform_post(
            '/action/{}'.format(uri_part),
            data=data
        )

    def _perform_upload(self, filepath):
        url = self._make_url('/action/upload')

        self._logger.info(
            'Загружается файл {!r} по ссылке {!r}.'.format(
                filepath, url
            )
        )

        sleep(1)
        with open(filepath, 'rb') as f:
            return self._session.post(
                url=url,
                files={
                    'data': (
                        filepath,
                        f,
                        guess_type(filepath)
                    )
                }
            ).json()['data']

    def _perform_import_action(self, import_type, **kwargs):
        return self._perform_action(
            'import{}'.format(import_type),
            **kwargs
        ).get('data')

    def _perform_login(self):
        return self._perform_action(
            'login',
            data={
                    'login': self._login,
                    'password': self._password
                },
            action='login'
        )['data']

    def get_users_of_my_group(self) -> list:
        """Получение пользователей своей группы."""

        service_group_id = list(filter(
            lambda row: row['service'],
            self.get_from_rest_collection('group', active_progress=False)['group']
        ))[0]['id']
        user_id = (
            self._perform_login()
        )['id']
        user_group = self.get_from_rest_collection('user_group', active_progress=False)['user_group']
        my_groups = list(
            map(
                lambda row: row['group_id'],
                filter(
                    lambda row: row['user_id'] == user_id,
                    user_group
                )
            )
        )
        my_users = set(
            map(
                lambda row: row['user_id'],
                filter(
                    lambda row: row['group_id'] in my_groups,
                    user_group
                )
            )
        )
        my_users.add(user_id)

        if service_group_id in my_groups:
            return []
        else:
            return list(my_users)

    def get_from_rest_collection(self, table, *args, active_progress=True, **kwargs):
        result = {}
        self._perform_login()
        counter = 0
        step = 100000
        if active_progress:
            pbar = tqdm(desc=f'Getting data from table {table}')
        basic_request = 'rest/collection/{}?{}&'.format(
            table,
            '&'.join(
                ['%s=%s' % (key, value) for (key, value) in kwargs.items()]
            )
        )
        for row in args:
            basic_request += '&'.join(row)
            basic_request += '&'
        while True:
            request = basic_request
            request += f'start={counter}'\
                       f'&stop={counter + step}'
            temp = self._perform_get(request)
            if active_progress:
                pbar.total = temp['meta']['count']
            counter += step
            if active_progress:
                pbar.update(min(
                    step,
                    temp['meta']['count'] - (counter - step)
                ))
            for table in temp:
                if table == 'meta':
                    continue
                if table not in result:
                    result[table] = []
                result[table] += temp[table]
            if counter >= temp['meta']['count']:
                break
        for table in result:
            result[table] = list({v['id']: v for v in result[table]}.values())
        return result

    def _get_orders(self, plan_id, *args, **kwargs):
        kwargs['column_names'] = 'plan_id'
        kwargs['search'] = plan_id
        kwargs['search_type'] = 1
        return self._perform_get(
            'rest/order?'+'&'.join(
                ['%s=%s' % (key, value) for (key, value) in kwargs.items()])
        )

    def perform_plan_import(self, filepath, plan_type):
        logger = self._logger

        logger.info('Импорт плана запущен.')

        result = self._perform_import_action(
            '/plan',
            data={
                'plan': {
                    'type': plan_type,
                    'name': self._make_entity_name(filepath)
                },
                'filepath': self._perform_upload(filepath),
                'aggregate_order_entries': True,
                'time_zone': self._time_zone
            }
        )
        return result

    def perform_delete_plan(self, plan_id):
        self._perform_delete(
            f'rest/plan/{plan_id}'
        )

    def create_static_calculation(self, start_date, stop_date,
                                  plan_id, user_id, wip=False):

        try:
            wip_date_info = self._perform_get(
                '/data/last_import_session?import_type=23'
            )['data'][0]['stop_stamp']
        except IndexError:
            wip_date_info = None

        if wip:

            entity_batch_snapshot_info = self._perform_action(
                f'entity_batch_snapshot/{wip_date_info}',
                data=None
            )
            ws = create_connection(
                f'{self._ws_url}/message',
                sslopt={'cert_reqs': ssl.CERT_NONE}
            )

            try:
                if 'data' in entity_batch_snapshot_info:
                    entity_batch_snapshot_id = entity_batch_snapshot_info['data']['id']
                else:
                    entity_batch_snapshot_id = entity_batch_snapshot_info['errors'][0]['description']['id']
                check_allocation = self._perform_action(
                    'state_allocation/check',
                    data={
                        'plan_id': plan_id,
                        'allocation_types': [{
                            'type': 0,
                            'entity_snapshot_id': entity_batch_snapshot_id
                        }]
                    }
                )['data'][0]['data']['allocated']

                if not check_allocation:
                    temp_uuid = str(uuid.uuid4())
                    allocate = self._perform_action(
                        'state_allocation/allocate',
                        data={
                            'state_allocation_session_uuid': temp_uuid,
                            'plan_id': plan_id,
                            'allocation_types': [{
                                'type': 0,
                                'entity_snapshot_id': entity_batch_snapshot_id
                            }]
                        }
                    )
                    while True:
                        message = json.loads(ws.recv())
                        print(str(message))
                        if message['msg'] == 'STATE_ALLOCATION_COMPLETED':
                            print(
                                self._perform_get(
                                    f'temporary/{message["data"]["result_temporary_key"]}'
                                )
                            )
                            break
                        if message['msg'] == 'STATE_ALLOCATION_FAILED':
                            raise
                    sleep(10)
                check_allocation = self._perform_action(
                    'state_allocation/check',
                    data={
                        'plan_id': plan_id,
                        'allocation_types': [{
                            'type': 0,
                            'entity_snapshot_id': entity_batch_snapshot_id
                        }]
                    }
                )['data'][0]['data']['allocated']

            except TypeError:
                tqdm.write(f"There is no WIP. Executing the calculation without WIP")
                entity_batch_snapshot_id = None

        else:
            entity_batch_snapshot_id = None

        static_session_id = self._perform_post(
            '/rest/static_session',
            data={
                'static_session': {
                    'data': None,
                    'entity_batch_snapshot_id': entity_batch_snapshot_id,
                    'entity_batch_variation_id': None,
                    'entity_route_variation_id': None,
                    'plan_id': plan_id,
                    'user_id': user_id,
                    'type': 0,
                    'working_time_ratio_variation_id': None,
                    'time_zone': self._time_zone
                }
            }
        )['static_session']['id']

        self._perform_action(
            'static',
            data={
                'action': None,
                'data': static_session_id
            }
        )

        return static_session_id

    def perform_delete_static_session(self, static_session_id):
        self._perform_delete(
            f'rest/static_session/{static_session_id}'
        )

    def _get_from_rest_collection(self, table):
        if table not in self.cache:
            self.cache[table] = []
            self._perform_login()
            counter = 0
            step = 100000
            if table == 'specification_item':
                order_by = '&order_by=parent_id&order_by=child_id'
            elif table == 'operation_profession':
                order_by = '&order_by=operation_id&order_by=profession_id'
            else:
                order_by = '&order_by=id'
            pbar = tqdm(desc=f'Getting data from table {table}')
            while True:
                temp = self._perform_get(
                    f'rest/collection/{table}'
                    f'?start={counter}'
                    f'&stop={counter + step}'
                    f'{order_by}'
                )
                pbar.total = temp['meta']['count']
                counter += step
                pbar.update(min(
                    step,
                    temp['meta']['count'] - (counter - step)
                ))
                if table not in temp:
                    break
                self.cache[table] += temp[table]
                if counter >= temp['meta']['count']:
                    break
        return self.cache[table]

    def get_spec(self, entity_id):
        if 'spec' in self.cache:
            return self.cache['spec'].get(int(entity_id)) or {}

        self.cache['spec'] = {}

        spec_list = self._get_from_rest_collection('specification_item')

        for row in spec_list:
            if row['parent_id'] not in self.cache['spec']:
                self.cache['spec'][row['parent_id']] = {}
            self.cache['spec'][
                row['parent_id']
            ][row['child_id']] = row['amount']

        return self.cache['spec'].get(int(entity_id)) or {}

    def get_last_department(self, dept_list, entity_id):
        if 'last_department' in self.cache:
            return self.cache['last_department'][int(entity_id)]

        self.cache['last_department'] = {}

        entity_routes_dict = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )

        department_dict = list_to_dict(
            self._get_from_rest_collection('department')
        )

        operation_dict = sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: k['nop']
        )

        for row in operation_dict:
            department = department_dict[row['department_id']]['identity']
            if dept_list is None:
                ...
            elif department not in dept_list:
                continue
            self.cache['last_department'][
                entity_routes_dict[row['entity_route_id']]['entity_id']
            ] = department

        return self.cache['last_department'][int(entity_id)]


    @classmethod
    def from_config(cls, config):
        return cls(
            config['input']['login'],
            str(config['input']['password']),
            config['input']['url'],
            config['input']['verify'],
            config['input']['ws_url'],
            config['input']['time_zone']
        )
