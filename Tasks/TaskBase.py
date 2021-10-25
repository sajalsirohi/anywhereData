class Task:
    __doc__ = """ 
    Abstract class that defines the behavior and the attributes of a task.
    These tasks can be created as a part of a pipeline also
    """

    def __init__(self, config, **options):
        self.options = options
        # ID of the task. These tasks then can be linked together to build a pipeline
        self.id = config.get('id')
        # task description of what generally this task will do
        self.task_description = config.get('task_description')
        # name of the source connection from which we want to retrieve the data
        self.source_connection_name = config.get('source_connection_name')
        # name of the target connection in which we want to store the data
        self.target_connection_name = config.get('target_connection_name')
        # source raw query, that will be executed on source connection. This can be
        # type of query. Postgres specific, mongodb specific, dynamo db specific etc.
        self.raw_query = config.get('raw_query')
        # raw_alias. This alias can be used to create the stage_query from. select 1,2 from raw_alias
        # this alias can only be used in stage_query
        self.raw_alias = config.get('raw_alias')
        # stage query that will be executed on the data extracted. It should only be in
        # SQL format. This SQL will be executed on the pandas dataframe in SQL, which then
        # will be final result that will be stored in target connection
        self.stage_query = config.get('stage_query')
        # stage alias. This alias is used to keep the result that we got from stage_query
        # which then can be further queried in another task
        self.stage_alias = config.get('stage_alias')
        # name of the target container where this modified data will be stored.
        self.target_container_name = config.get('target_container_name')
        # save mode of the data that is extracted. {'fail', 'replace', 'append'},
        self.save_mode = config.get('save_mode')
        # optional parameters will be given here.
        self.optional_param = config.get('optional_param', {})

    def __str__(self):
        return f"""
        ID                      : {self.id}
        Task Description        : {self.task_description}
        SRC Conn Name           : {self.source_connection_name}
        TGT Conn Name           : {self.target_connection_name}
        Raw Query               : {self.raw_query}
        Raw Alias               : {self.raw_alias}
        Stage Query             : {self.stage_query}
        Stage Alias             : {self.stage_alias}
        Target Container Name   : {self.target_container_name}
        Save Mode               : {self.save_mode}
        Optional Param          : {self.optional_param}
        Options                 : {self.options}
        """
