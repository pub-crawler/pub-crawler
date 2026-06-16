def job_id(job):
  id = "<not a job>"
  if isinstance(job, dict) and "job_type" in job:
    job_type = job.get("job_type")
    if job_type == "actor":
      id = f"{job_type}:{job.get("actor_id")}"
    elif job_type == "webfinger":
      id = f"{job_type}:{job.get("webfinger")}"
    elif job_type == "collection":
      id = f"{job_type}:{job.get("collection_id")}"
    elif job_type == "page":
      id = f"{job_type}:{job.get("page_id")}"
  return id
