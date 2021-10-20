import argparse

__all__ = ["additional_options"]


class keyValue(argparse.Action):
    def __call__(self, parser, namespace,
                 values, option_string=None):
        setattr(namespace, self.dest, dict())

        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value


parser = argparse.ArgumentParser()

parser.add_argument('--options',
                    nargs='*',
                    action=keyValue)

args = parser.parse_args()

additional_options = args.options

if additional_options is None:
    additional_options = {}