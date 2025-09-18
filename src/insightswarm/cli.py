import argparse
from .database.oslm_sqlite import OSLMSqliteController


def main() -> None:
    print("Hello from insightswarm!")
    
    parser = argparse.ArgumentParser(prog="tmp-insightswarm", description="Temporary MCP server for building oslm database")
    sub_parser = parser.add_subparsers(dest="command", help="Available commands")
    
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--config")
    
    mcp_parser = sub_parser.add_parser("mcp", parents=[parent_parser])
    mcp_parser.add_argument("--start", choices=['oslm-database'])
    mcp_parser.set_defaults(func=mcp_run)
    
    db_parser = sub_parser.add_parser("db", parents=[parent_parser])
    db_parser.add_argument("--init", choices=['oslm-sqlite'])
    db_parser.set_defaults(func=db_run)
    
    args = parser.parse_args()
    
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()
    
    
def mcp_run(args):
    if args.start:
        match args.start:
            case "oslm-database":
                from .mcp_server.oslm_db_mcp import start
                start()
            case _:
                raise NotImplementedError()


def db_run(args):
    if args.init:
        match args.init:
            case "oslm-sqlite":
                controller = OSLMSqliteController()
                controller.init()
            case _:
                raise NotImplementedError()
