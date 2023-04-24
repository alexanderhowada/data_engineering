import sys
sys.path.append('../')

from utils.url_utils import BaseAPI


class ClimaTempoAPI(BaseAPI):

    # http://apiadvisor.climatempo.com.br/doc/index.html
    base = 'http://apiadvisor.climatempo.com.br'
    endpoints = {
        'list_cities': f'{base}/api/v1/locale/city',
        'forecast_72': base + '/api/v1/forecast/locale/{cid}/hours/72',
        'register_id': base + '/api-manager/user-token/{token}/locales'
    }

    def __init__(self, token, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.token = token

    def _prepare_params(self, params: dict = {}):

        if 'token' not in params.keys():
            params['token'] = self.token

        return params

    def get_request(self, url: str, **r_kwargs):

        if 'params' not in r_kwargs.keys():
            r_kwargs['params'] = self._prepare_params()
        else:
            r_kwargs['params'] = self._prepare_params(params=r_kwargs['params'])

        return super().get_request(url, **r_kwargs)


    def forecast_72(self, city_ids, **r_kwargs):

        json_list = []

        for cid in city_ids:
            endpoint = self.endpoints['forecast_72'].format(cid=cid)
            json_list.append(self.get_json(endpoint, **r_kwargs))

        return json_list

    def register_id(self, city_ids, **p_kwargs):

        put_list = []

        for cid in city_ids:
            endpoint = self.endpoints['register_id'].format(token=self.token)
            put_list.append(self.session.put(endpoint, data={'id': cid}, **p_kwargs))

        return put_list




if __name__ == '__main__':

    import os

    TOKEN = os.environ['TOKEN']

    api = ClimaTempoAPI(TOKEN)

    # List cities example
    # r_kwargs = {
    #     'timeout': 60,
    #     'params': {
    #         # 'country': 'CO',
    #         'country': 'BR'
    #     }
    # }
    #
    # j = api.get_json('list_cities', **r_kwargs)
    # print(j)

    # Register city
    # p_kwargs = {'timeout': 60}
    # p_list = api.register_id(['3477'])
    # print(p_list[0].json())
    # print(p_list)
    # print(len(p_list))

    # List cities example
    r_kwargs = {
        'timeout': 60
    }

    j = api.forecast_72([3477], **r_kwargs) # testing for SP and Sogamoso
    print(j)
    print(len(j))