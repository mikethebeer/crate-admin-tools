import argh
from .replicas import replicas
from .optimize import optimize


def main():
    parser = argh.ArghParser()
    parser.add_commands([replicas, optimize])
    parser.dispatch()


if __name__ == "__main__":
    main()
