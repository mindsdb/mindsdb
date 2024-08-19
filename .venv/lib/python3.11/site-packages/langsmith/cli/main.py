import argparse
import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Union, cast

from langsmith import env as ls_env
from langsmith import utils as ls_utils

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

_DIR = Path(__file__).parent


def pprint_services(services_status: List[Mapping[str, Union[str, List[str]]]]) -> None:
    # Loop through and collect Service, State, and Publishers["PublishedPorts"]
    # for each service
    services = []
    for service in services_status:
        service_status: Dict[str, str] = {
            "Service": str(service["Service"]),
            "Status": str(service["Status"]),
        }
        publishers = cast(List[Dict], service.get("Publishers", []))
        if publishers:
            service_status["PublishedPorts"] = ", ".join(
                [str(publisher["PublishedPort"]) for publisher in publishers]
            )
        services.append(service_status)

    max_service_len = max(len(service["Service"]) for service in services)
    max_state_len = max(len(service["Status"]) for service in services)
    service_message = [
        "\n"
        + "Service".ljust(max_service_len + 2)
        + "Status".ljust(max_state_len + 2)
        + "Published Ports"
    ]
    for service in services:
        service_str = service["Service"].ljust(max_service_len + 2)
        state_str = service["Status"].ljust(max_state_len + 2)
        ports_str = service.get("PublishedPorts", "")
        service_message.append(service_str + state_str + ports_str)

    service_message.append(
        "\nTo connect, set the following environment variables"
        " in your LangChain application:"
        "\nLANGSMITH_TRACING_V2=true"
        "\nLANGSMITH_ENDPOINT=http://localhost:80/api"
    )
    logger.info("\n".join(service_message))


class LangSmithCommand:
    """Manage the LangSmith Tracing server."""

    def __init__(self) -> None:
        self.docker_compose_file = (
            Path(__file__).absolute().parent / "docker-compose.yaml"
        )

    @property
    def docker_compose_command(self) -> List[str]:
        return ls_utils.get_docker_compose_command()

    def _open_browser(self, url: str) -> None:
        try:
            subprocess.run(["open", url])
        except FileNotFoundError:
            pass

    def _start_local(self) -> None:
        command = [
            *self.docker_compose_command,
            "-f",
            str(self.docker_compose_file),
        ]
        subprocess.run(
            [
                *command,
                "up",
                "--quiet-pull",
                "--wait",
            ]
        )
        logger.info(
            "LangSmith server is running at http://localhost:80/api.\n"
            "To view the app, navigate your browser to http://localhost:80"
            "\n\nTo connect your LangChain application to the server"
            " locally,\nset the following environment variable"
            " when running your LangChain application.\n"
        )

        logger.info("\tLANGSMITH_TRACING=true")
        logger.info("\tLANGSMITH_ENDPOINT=http://localhost:80/api\n")
        self._open_browser("http://localhost")

    def pull(
        self,
        *,
        version: str = "0.5.7",
    ) -> None:
        """Pull the latest LangSmith images.

        Args:
            version: The LangSmith version to use for LangSmith. Defaults to 0.5.7
        """
        os.environ["_LANGSMITH_IMAGE_VERSION"] = version
        subprocess.run(
            [
                *self.docker_compose_command,
                "-f",
                str(self.docker_compose_file),
                "pull",
            ]
        )

    def start(
        self,
        *,
        openai_api_key: Optional[str] = None,
        langsmith_license_key: str,
        version: str = "0.5.7",
    ) -> None:
        """Run the LangSmith server locally.

        Args:
            openai_api_key: The OpenAI API key to use for LangSmith
                If not provided, the OpenAI API Key will be read from the
                OPENAI_API_KEY environment variable. If neither are provided,
                some features of LangSmith will not be available.
            langsmith_license_key: The LangSmith license key to use for LangSmith
                If not provided, the LangSmith license key will be read from the
                LANGSMITH_LICENSE_KEY environment variable. If neither are provided,
                Langsmith will not start up.
            version: The LangSmith version to use for LangSmith. Defaults to latest.
        """
        if openai_api_key is not None:
            os.environ["OPENAI_API_KEY"] = openai_api_key
        if langsmith_license_key is not None:
            os.environ["LANGSMITH_LICENSE_KEY"] = langsmith_license_key
        self.pull(version=version)
        self._start_local()

    def stop(self, clear_volumes: bool = False) -> None:
        """Stop the LangSmith server."""
        cmd = [
            *self.docker_compose_command,
            "-f",
            str(self.docker_compose_file),
            "down",
        ]
        if clear_volumes:
            confirm = input(
                "You are about to delete all the locally cached "
                "LangSmith containers and volumes. "
                "This operation cannot be undone. Are you sure? [y/N]"
            )
            if confirm.lower() != "y":
                print("Aborting.")
                return
            cmd.append("--volumes")

        subprocess.run(cmd)

    def logs(self) -> None:
        """Print the logs from the LangSmith server."""
        subprocess.run(
            [
                *self.docker_compose_command,
                "-f",
                str(self.docker_compose_file),
                "logs",
            ]
        )

    def status(self) -> None:
        """Provide information about the status LangSmith server."""
        command = [
            *self.docker_compose_command,
            "-f",
            str(self.docker_compose_file),
            "ps",
            "--format",
            "json",
        ]

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            command_stdout = result.stdout.decode("utf-8")
            services_status = json.loads(command_stdout)
        except json.JSONDecodeError:
            logger.error("Error checking LangSmith server status.")
            return
        if services_status:
            logger.info("The LangSmith server is currently running.")
            pprint_services(services_status)
        else:
            logger.info("The LangSmith server is not running.")
            return


def env() -> None:
    """Print the runtime environment information."""
    env = ls_env.get_runtime_environment()
    env.update(ls_env.get_docker_environment())
    env.update(ls_env.get_langchain_env_vars())

    # calculate the max length of keys
    max_key_length = max(len(key) for key in env.keys())

    logger.info("LangChain Environment:")
    for k, v in env.items():
        logger.info(f"{k:{max_key_length}}: {v}")


def main() -> None:
    """Main entrypoint for the CLI."""
    print("BY USING THIS SOFTWARE YOU AGREE TO THE TERMS OF SERVICE AT:")
    print("https://smith.langchain.com/terms-of-service.pdf")

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(description="LangSmith CLI commands")

    server_command = LangSmithCommand()
    server_start_parser = subparsers.add_parser(
        "start", description="Start the LangSmith server."
    )
    server_start_parser.add_argument(
        "--openai-api-key",
        default=os.getenv("OPENAI_API_KEY"),
        help="The OpenAI API key to use for LangSmith."
        " If not provided, the OpenAI API Key will be read from the"
        " OPENAI_API_KEY environment variable. If neither are provided,"
        " some features of LangSmith will not be available.",
    )
    server_start_parser.add_argument(
        "--langsmith-license-key",
        default=os.getenv("LANGSMITH_LICENSE_KEY"),
        help="The LangSmith license key to use for LangSmith."
        " If not provided, the LangSmith License Key will be read from the"
        " LANGSMITH_LICENSE_KEY environment variable. If neither are provided,"
        " the Langsmith application will not spin up.",
    )
    server_start_parser.add_argument(
        "--version",
        default="0.5.7",
        help="The LangSmith version to use for LangSmith. Defaults to 0.5.7.",
    )
    server_start_parser.set_defaults(
        func=lambda args: server_command.start(
            openai_api_key=args.openai_api_key,
            langsmith_license_key=args.langsmith_license_key,
            version=args.version,
        )
    )

    server_stop_parser = subparsers.add_parser(
        "stop", description="Stop the LangSmith server."
    )
    server_stop_parser.add_argument(
        "--clear-volumes",
        action="store_true",
        help="Delete all the locally cached LangSmith containers and volumes.",
    )
    server_stop_parser.set_defaults(
        func=lambda args: server_command.stop(clear_volumes=args.clear_volumes)
    )

    server_pull_parser = subparsers.add_parser(
        "pull", description="Pull the latest LangSmith images."
    )
    server_pull_parser.add_argument(
        "--version",
        default="0.5.7",
        help="The LangSmith version to use for LangSmith. Defaults to 0.5.7.",
    )
    server_pull_parser.set_defaults(
        func=lambda args: server_command.pull(version=args.version)
    )
    server_logs_parser = subparsers.add_parser(
        "logs", description="Show the LangSmith server logs."
    )
    server_logs_parser.set_defaults(func=lambda args: server_command.logs())
    server_status_parser = subparsers.add_parser(
        "status", description="Show the LangSmith server status."
    )
    server_status_parser.set_defaults(func=lambda args: server_command.status())
    env_parser = subparsers.add_parser("env")
    env_parser.set_defaults(func=lambda args: env())

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
