#!/usr/bin/env python

import os
import time
import requests
from operator import itemgetter
import sys


DEVLAKE_ENDPOINT = os.getenv('DEVLAKE_ENDPOINT')


def error_handler(func):
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                print(f'Function error "{func.__name__}": {str(e)}')
                sys.exit(1)
        return wrapper


class DevlakeQueue:

    
    def __init__(self):
        self.precheck()        

    @error_handler
    def precheck(self):
        assert os.environ.get('DEVLAKE_ENDPOINT') is not None, 'environment "DEVLAKE_ENDPOINT" is not set'


    @error_handler
    def request_handler(self, **kwargs):

        path = kwargs.get('path', None)
        method = kwargs.get('method', None)
        page = kwargs.get('page', 1)
        page_size = kwargs.get('page_size', None)

        assert path is not None, '"path" must not be None!'
        assert method is not None, '"method" must not be None!'
        assert page_size is not None, '"page_size" must not be None!'

        params = {
            'pageSize': page_size,
            'page': page
        }

        url = f'{DEVLAKE_ENDPOINT}{path}'
        req = requests.Request(method, url, params=params)
        prep = req.prepare()
        res = requests.Session().send(prep)

        if res.status_code >= 200 and res.status_code < 300:
            data = res.json()
            if isinstance(data, dict) or isinstance(data, list):
                return res.json()
            else:
                raise ValueError("Invalid data type")
        else:
            raise ValueError(f'({res.status_code}) {res.reason}')

    
    @error_handler
    def get_blueprints(self):

        # set pageSize by the number of blueprints
        page_size = self.request_handler(
            path='/blueprints?pageSize=1',
            method='GET',
            page_size=1)['count']

        # get all blueprints
        blueprints = self.request_handler(
            path='/blueprints',
            method='GET',
            page_size=page_size)['blueprints']

        # get blueprints ids
        blueprint_ids = [x['id'] for x in blueprints]

        return blueprint_ids
    

    @error_handler
    def get_pipelines(self):

        # set pageSize by the number of pipelines
        page_size = self.request_handler(
            path='/pipelines?pageSize=1',
            method='GET',
            page_size=1)['count']

        # get all pipelines
        pipelines = self.request_handler(
            path='/pipelines',
            method='GET',
            page_size=page_size)['pipelines']

        # order blueprint by pipeline execution time
        last_pipeline_by_blueprints = {}
        for pipeline in pipelines:
            blueprint_id = pipeline['blueprintId']
            finished_at = pipeline['finishedAt']
            if finished_at is not None:
                if blueprint_id not in last_pipeline_by_blueprints:
                    last_pipeline_by_blueprints[blueprint_id] = finished_at
                else:
                    if finished_at > last_pipeline_by_blueprints[blueprint_id]:
                        last_pipeline_by_blueprints[blueprint_id] = finished_at
        
        # remove from blueprints that not exists
        blueprints_to_remove =  list(set(last_pipeline_by_blueprints.keys()) ^ set(self.get_blueprints()))
        for b in blueprints_to_remove:
            last_pipeline_by_blueprints.pop(b)

        # sort by datime
        sorted_blueprints = dict(sorted(last_pipeline_by_blueprints.items(), key=itemgetter(1)))
        
        return list(sorted_blueprints.keys())


    @error_handler
    def there_some_pipeline_running(self):
        
        pipeline_running = self.request_handler(
            path='/pipelines?status=TASK_RUNNING',
            method='GET',
            page_size=10)['pipelines']
        
        if len(pipeline_running) > 0:
            self.blueprint_in_execution = [ x['blueprintId'] for x in pipeline_running]
            return True
        else:
            return False
                
    
    @error_handler
    def trigget_blueprint(self, blueprint_id):
        
        self.request_handler(
            path=f'/blueprints/{blueprint_id}/trigger',
            method='POST',
            page_size=1)
        
        time.sleep(5) # wait five seconds to prevent the loop to trigger another blueprint

    
    @error_handler
    def run_queue(self):
        
        queue = self.get_pipelines()
        
        if len(queue) == 0:
            print('Queue is empty!')
        else:
            for blueprint_id in queue:
                while self.there_some_pipeline_running():
                    print(f'Waiting for pipeline running! (blueprintId: {self.blueprint_in_execution})')
                    time.sleep(5)
                self.trigget_blueprint(blueprint_id)

    
if __name__ == '__main__':
    dq = DevlakeQueue()
    while True:
        dq.run_queue()
        time.sleep(60)