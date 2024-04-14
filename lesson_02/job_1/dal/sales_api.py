import httpx

from lesson_02.job_1.settings import SALES_ENDPOINT, AUTH_TOKEN


def get_sales(date: str) -> list[dict[str, str | int]]:
    """
    Get data from sales API for specified date.

    :param date: date to retrieve the data from
    :return: list of records
    """
    data = []
    headers = {'Authorization': AUTH_TOKEN}
    page = 1
    params = {'date': date, 'page': page}
    with httpx.Client() as client:
        response = client.get(SALES_ENDPOINT, params=params, headers=headers)
        while response.status_code != 404 or not (len(response.json()) < 100):
            data.extend(response.json())
            page += 1
            params = {'date': date, 'page': page}
            response = client.get(SALES_ENDPOINT, params=params, headers=headers)
    print(f'Collected {page - 1} pages with {len(data)} records.') if data else print(f'No records found')
    return data


if __name__ == '__main__':
    get_sales('2022-08-10')
