from flask import Flask, request
from flask import typing as flask_typing

from lesson_02.job_2.bll.staging import raw_to_stage


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    POST body in JSON:
    {
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09",
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data = request.json
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    if not raw_dir or not stg_dir:
        return {
            "message": f"{'raw_dir' if not raw_dir else 'stg_dir'} parameter missed"
        }, 400

    raw_to_stage(raw_dir=raw_dir, stg_dir=stg_dir)

    return {
        "message": f"Files processed successfully"
    }, 201


if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=8082)
