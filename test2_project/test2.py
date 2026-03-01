import datetime
import sys
import io
import re
from rich.console import Console
from rich.table import Table
from rich.box import ASCII

buf = io.StringIO()
console = Console(file=buf, force_terminal=True, no_color=True, width=80)

table = Table(title="Test Script 2", box=ASCII)
table.add_column("Timestamp")
table.add_column("Status")
table.add_row(str(datetime.datetime.now()), "Success")

console.print(table)

output = re.sub(r"\x1b\[[0-9;]*m", "", buf.getvalue())
print(output, end="")
