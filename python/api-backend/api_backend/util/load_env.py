from os.path import abspath, dirname, join

from dotenv import load_dotenv


def load_env() -> None:
    """
    Loads environment variables from .env file.

    The function reads environment variables from a .env file in the project directory and loads them into the
    environment so that they can be accessed using os.environ.

    Returns: None.
    """
    file_path = abspath(__file__)
    parent_dir = dirname(file_path)
    env_path = join(parent_dir, "..", "..", ".env")
    load_dotenv(env_path)
