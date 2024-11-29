from yaml import load, SafeLoader


def read_config(config_filepath):
    with open(config_filepath, 'r', encoding="utf-8") as f:
        return load(f, Loader=SafeLoader)
