from prefect.deployments.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from parametrized_flow_gtb import etl_parent_flow_gtb


docker_block = DockerContainer.load("taxi-etl")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow_gtb,
    name="docker-flow",
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()
