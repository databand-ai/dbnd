# DO WE REALLY NEED extra configuration system before the main one is up?
#
# import yaml
# from dbnd._core.utils.project.project_fs import project_path
# from dbnd._core.utils.singleton_context import SingletonContext
#
#
# class DatabandProject(SingletonContext):
#
#     def __init__(self, config):
#         super(DatabandProject, self).__init__()
#         self._config = config
#         self.name = config['name']
#
#
# def get_project_config():
#     """
#     """
#     if not DatabandProject.has_instance():
#         project_config_path = project_path("databand.yml")
#         config = yaml.safe_load(open(project_config_path).read())
#
#         DatabandProject.try_instance(config)
#
#     return DatabandProject.get_instance()
