import os

import httpx


API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app'
SALES_ENDPOINT = f'{API_URL}/sales'
AUTH_TOKEN = os.environ.get('AUTH_TOKEN')


def get_sales(date: str) -> list[dict[str, str | int]]:
    """
    Get data from sales API for specified date.

    :param date: date to retrieve the data from
    :return: list of records
    """
    data = []
    headers = {'Authorization': AUTH_TOKEN}
    page = 1
    while True:
        params = {'date': date, 'page': page}
        with httpx.Client() as client:
            response = client.get(SALES_ENDPOINT, params=params, headers=headers)
            if response.status_code == 404:  # no data or no more pages to get
                if page == 1:
                    print('No records found')
                else:
                    print(f'Collected {page} pages with {len(data)} records.')
                break
            data.extend(response.json())
            if len(response.json()) < 100:  # last page for sure
                print(f'Collected {page} pages with {len(data)} records.')
                break
            page += 1
    return data


if __name__ == '__main__':
    get_sales('2022-08-10')
