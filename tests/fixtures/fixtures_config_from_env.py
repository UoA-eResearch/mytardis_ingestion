from pytest import fixture

from src.helpers.config import (
    GeneralConfig,
    AuthConfig,
    ConnectionConfig,
    StorageConfig,
    SchemaConfig,
    ProxyConfig,
    IntrospectionConfig,
    ConfigFromEnv,
)


@fixture
def processed_introspection_response():
    return {
        "old_acls": False,
        "projects_enabled": True,
        "objects_with_ids": [
            "dataset",
            "experiment",
            "facility",
            "instrument",
            "project",
            "institution",
        ],
    }


@fixture
def general(default_institution) -> GeneralConfig:
    return GeneralConfig(default_institution=default_institution)


@fixture
def auth(username, api_key) -> AuthConfig:
    return AuthConfig(username=username, api_key=api_key)


@fixture
def connection(hostname, proxies, verify_certificate) -> ConnectionConfig:
    return ConnectionConfig(
        hostname=hostname,
        proxy=ProxyConfig(http=proxies["http"], https=proxies["https"]),
        verify_certificate=verify_certificate,
    )


@fixture
def storage(storage_box, source_dir, target_dir) -> StorageConfig:
    return StorageConfig(
        box=storage_box.name,
        source_directory=source_dir,
        target_directory=target_dir,
    )


@fixture
def default_schema(
    project_schema, experiment_schema, dataset_schema, datafile_schema
) -> SchemaConfig:
    return SchemaConfig(
        project=project_schema,
        experiment=experiment_schema,
        dataset=dataset_schema,
        datafile=datafile_schema,
    )


@fixture
def mytardis_setup(processed_introspection_response) -> IntrospectionConfig:
    return IntrospectionConfig(
        old_acls=processed_introspection_response["old_acls"],
        projects_enabled=processed_introspection_response["projects_enabled"],
        objects_with_ids=processed_introspection_response["objects_with_ids"],
    )


@fixture
def mytardis_settings(
    general: GeneralConfig,
    auth: AuthConfig,
    connection: ConnectionConfig,
    storage: StorageConfig,
    default_schema: SchemaConfig,
) -> ConfigFromEnv:
    return ConfigFromEnv(
        general=general,
        auth=auth,
        connection=connection,
        storage=storage,
        default_schema=default_schema,
    )
