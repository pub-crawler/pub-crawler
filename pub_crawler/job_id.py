def job_id(job):
  id = "<not a job>"
  if isinstance(job, dict) and "type" in job:
    type = job.get("type")
    if type == "actor":
      id = f"{type}:{job.get("actor_id")}"
    elif type == "webfinger":
      id = f"{type}:{job.get("webfinger")}"
    elif type == "collection":
      id = f"{type}:{job.get("collection_id")}"
    elif type == "page":
      id = f"{type}:{job.get("page_id")}"
  return id
