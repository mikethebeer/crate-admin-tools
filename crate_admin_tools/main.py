import argh
from .replicas import replicas


def main():
    parser = argh.ArghParser()
    parser.add_commands([replicas])
    parser.dispatch()


if __name__ == "__main__":
    main()
