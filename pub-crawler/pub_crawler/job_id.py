def job_id(job):
    id = None
    if isinstance(job, dict) and "job_type" in job:
        job_type = job.get("job_type")
        if job_type == "actor" and "actor_id" in job:
            id = f"{job_type}:{job.get("actor_id")}"
        elif job_type == "webfinger" and "webfinger" in job:
            id = f"{job_type}:{job.get("webfinger")}"
        elif job_type == "collection" and "collection_id" in job:
            id = f"{job_type}:{job.get("collection_id")}"
        elif job_type == "page" and "page_id" in job:
            id = f"{job_type}:{job.get("page_id")}"
    return id
