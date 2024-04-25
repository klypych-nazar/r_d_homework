import os
import time
import httpx
import click


BASE_DIR = os.environ.get("BASE_DIR")

if not BASE_DIR:
    print("BASE_DIR environment variable must be set")
    exit(1)

JOB1_PORT = 8081
JOB2_PORT = 8082


def run_job1(date, raw_dir):
    print("Starting job1:")
    resp = httpx.post(
        url=f'http://localhost:{JOB1_PORT}/',
        json={
            "date": date,
            "raw_dir": raw_dir
        }
    )
    assert resp.status_code == 201
    print("job1 completed!")


def run_job2(raw_dir, stg_dir):
    print("Starting job2:")
    resp = httpx.post(
        url=f'http://localhost:{JOB2_PORT}/',
        json={
            "raw_dir": raw_dir,
            "stg_dir": stg_dir
        }
    )
    assert resp.status_code == 201
    print("job2 completed!")

@click.command()
@click.option('--date', '-d', type=str, help='date in the format YYYY-MM-DD')
def main(date='2022-08-09'):
    raw_dir = os.path.join(BASE_DIR, "raw", "sales", date)
    stg_dir = os.path.join(BASE_DIR, "stg", "sales", date)

    run_job1(date, raw_dir)
    time.sleep(3)
    run_job2(raw_dir, stg_dir)


if __name__ == '__main__':
    main()
