def clickable_glue_links(job, logger):

    jr = job.job_status["JobRun"]["Id"]

    # Error
    url_start = "https://eu-west-1.console.aws.amazon.com/cloudwatch/home?region=eu-west-1#logStream:"
    group = "group=/aws-glue/jobs/error;"
    prefix = f"prefix={jr};"
    stream = "streamFilter=typeLogStreamPrefix"
    glue_logs_error = f"{url_start}{group}{prefix}{stream}"

    # Output
    url_start = "https://eu-west-1.console.aws.amazon.com/cloudwatch/home?region=eu-west-1#logsV2:log-groups/log-group/"

    job = f"$252Faws-glue$252Fjobs$252Foutput$3FlogStreamNameFilter$3D{jr}"
    glue_logs_output = f"{url_start}{job}"

    # Logs
    url_start = "https://eu-west-1.console.aws.amazon.com/cloudwatch/home?region=eu-west-1#logsV2:log-groups/log-group/"
    job = f"$252Faws-glue$252Fjobs$252Flogs-v2$3FlogStreamNameFilter$3D{jr}"
    glue_logs_logs = f"{url_start}{job}"
    outputs = []
    outputs.append(f"**Clickable Glue job links:***")
    outputs.append("    Glue console")
    outputs.append(
        "    https://eu-west-1.console.aws.amazon.com/glue/home?region=eu-west-1#etl:tab=jobs"
    )
    outputs.append("    Watchtower link: Glue outputs errors)")
    outputs.append(f"    {glue_logs_output}")
    outputs.append("    Watchtower link: Glue logs")
    outputs.append(f"    {glue_logs_logs}")
    outputs.append("    Watchtower link: Legacy glue errors")
    outputs.append(f"    {glue_logs_error}")
    outputs.append("")
    outputs.append("")

    message = "\n".join(outputs)
    logger.info(message)
