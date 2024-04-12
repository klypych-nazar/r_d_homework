from lesson_02.job_2.dal import local_disk, transform


def raw_to_stage(raw_dir: str, stg_dir: str) -> None:
    """
    Processes data from a raw JSON file located in 'raw_dir', transforms it to Avro format,
    and saves it in the 'stg_dir' directory.
    """
    # 1. get json_content from raw data
    json_content: dict = local_disk.read_from_disk(raw_dir=raw_dir)
    if not json_content:
        return

    # 2. Create or clean dir tree for stg_dir
    local_disk.manage_directories(stg_dir)

    # 3. Compose file name based on stage_dir
    file_name: str = local_disk.add_file_name_to_path(stg_dir, file_type='avro')

    # 4. Transform json to avro and write to a file
    transform.json_to_avro(json_content, file_name=file_name)
