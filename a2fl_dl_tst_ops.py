"""
A2DB Comprehensive Performance Test - Flight Client Operations
================================================================

Tests CRUD operations using module-level API:
- a2db_ins() for INSERT
- a2db_sel() for SELECT
- a2db_updt() for UPDATE
- a2db_del() for DELETE

Run from: C:\a2flight_client
"""

import asyncio
import os
import time
import gc
import psutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import random
import sys

import pyarrow as pa
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich import box
from rich.text import Text

# Add a2flight_cl to path
sys.path.insert(0, str(Path(__file__).parent))

# Import module-level API
import a2flight_client as a2fl

# Configuration
TEST_TABLE = "a2db_test_tbl"
TEST_BATCH_SIZES = [1000, 15000, 50000, 150000, 300000, 500000, 750000, 1000000, 1500000]

# Set environment variable for config
config_path = Path(__file__).parent / "a2flight_cl.env"
if config_path.exists():
    os.environ['A2FLIGHT_CLIENT_CONFIG_PATH'] = str(config_path.absolute())

console = Console()


@dataclass
class OperationMetrics:
    """Comprehensive metrics for a single operation."""
    operation: str
    batch_size: int
    rows_affected: int
    execution_time_ms: float
    throughput_rps: float
    memory_before_mb: float
    memory_after_mb: float
    memory_used_mb: float
    memory_per_op_kb: float
    method_used: str
    timestamp: datetime = field(default_factory=datetime.now)
    success: bool = True
    error_message: Optional[str] = None

    @property
    def execution_time_per_op_ms(self) -> float:
        """Execution time per operation in milliseconds."""
        return self.execution_time_ms / max(self.rows_affected, 1)

    @property
    def performance_grade(self) -> str:
        """Calculate performance grade based on industry benchmarks."""
        if not self.success:
            return "F"

        if self.throughput_rps >= 1_000_000:
            return "A++"
        elif self.throughput_rps >= 500_000:
            return "A+"
        elif self.throughput_rps >= 250_000:
            return "A"
        elif self.throughput_rps >= 100_000:
            return "A-"
        elif self.throughput_rps >= 50_000:
            return "B+"
        elif self.throughput_rps >= 25_000:
            return "B"
        elif self.throughput_rps >= 10_000:
            return "B-"
        elif self.throughput_rps >= 5_000:
            return "C"
        elif self.throughput_rps >= 1_000:
            return "D"
        else:
            return "F"

    @property
    def meets_execution_target(self) -> bool:
        """Check if meets <5ms per operation target."""
        return self.execution_time_per_op_ms < 5.0

    @property
    def meets_memory_target(self) -> bool:
        """Check if meets <50KB per operation target."""
        return self.memory_per_op_kb < 50.0


class PerformanceTestRunner:
    """Comprehensive performance test runner for A2DB Flight operations."""

    def __init__(self):
        self.metrics: List[OperationMetrics] = []
        self.process = psutil.Process()

    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / 1024 / 1024

    def generate_test_data(self, num_rows: int) -> pa.Table:
        """Generate test data matching a2db_test_tbl schema."""
        base_time = datetime.now()

        schema = pa.schema([
            ('a2timestamp', pa.timestamp('us', tz='UTC')),
            ('device_id', pa.string()),
            ('sensor_id', pa.string()),
            ('event_type', pa.string()),
            ('metric_name', pa.string()),
            ('metric_value', pa.float64()),
            ('value_int', pa.int64()),
            ('value_bool', pa.bool_()),
            ('status_code', pa.int32()),
            ('latitude', pa.float64()),
            ('longitude', pa.float64()),
            ('notes', pa.string()),
            ('firmware_version', pa.string()),
            ('created_at', pa.timestamp('us', tz='UTC'))
        ])

        data = [
            pa.array([base_time + timedelta(seconds=i) for i in range(num_rows)],
                     type=pa.timestamp('us', tz='UTC')),
            pa.array([f'device_{random.randint(1, 100):03d}' for _ in range(num_rows)]),
            pa.array([f'sensor_{random.randint(1, 50):03d}' for _ in range(num_rows)]),
            pa.array([random.choice(['temperature', 'pressure', 'motion', 'alert'])
                      for _ in range(num_rows)]),
            pa.array([random.choice(['temp', 'humidity', 'pressure', 'voltage'])
                      for _ in range(num_rows)]),
            pa.array([random.uniform(20.0, 30.0) for _ in range(num_rows)]),
            pa.array([random.randint(0, 1000) for _ in range(num_rows)], type=pa.int64()),
            pa.array([random.choice([True, False]) for _ in range(num_rows)]),
            pa.array([random.choice([200, 201, 400, 500]) for _ in range(num_rows)],
                     type=pa.int32()),
            pa.array([random.uniform(-90, 90) for _ in range(num_rows)]),
            pa.array([random.uniform(-180, 180) for _ in range(num_rows)]),
            pa.array([f'Test note {i}' for i in range(num_rows)]),
            pa.array([f'v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}'
                      for _ in range(num_rows)]),
            pa.array([base_time + timedelta(seconds=i) for i in range(num_rows)],
                     type=pa.timestamp('us', tz='UTC'))
        ]

        return pa.Table.from_arrays(data, schema=schema)

    async def clear_test_table(self):
        """Clear test table before starting tests."""
        try:
            console.print("\n[bold yellow]🧹 Clearing test table...[/bold yellow]")
            query = f"SELECT a2timestamp, device_id FROM {TEST_TABLE}"
            existing_data = await a2fl.a2db_sel(query)

            if existing_data.num_rows > 0:
                await a2fl.a2db_del(existing_data, TEST_TABLE)
                console.print(f"[bold green]✓ Cleared {existing_data.num_rows:,} existing rows[/bold green]\n")
            else:
                console.print("[bold green]✓ Table already empty[/bold green]\n")
        except Exception as e:
            console.print(f"[bold yellow]⚠️  Warning clearing table: {e}[/bold yellow]\n")

    async def test_insert(self, batch_size: int) -> OperationMetrics:
        """Test INSERT operation using a2db_ins()."""
        try:
            test_data = self.generate_test_data(batch_size)

            gc.collect()
            memory_before = self.get_memory_usage_mb()

            start_time = time.perf_counter()
            result = await a2fl.a2db_ins(test_data, TEST_TABLE)
            execution_time = (time.perf_counter() - start_time) * 1000

            memory_after = self.get_memory_usage_mb()
            memory_used = memory_after - memory_before

            rows_written = result.get('rows_written', batch_size)
            throughput_rps = rows_written / (execution_time / 1000) if execution_time > 0 else 0
            memory_per_op = (memory_used * 1024) / max(rows_written, 1)

            return OperationMetrics(
                operation="INSERT",
                batch_size=batch_size,
                rows_affected=rows_written,
                execution_time_ms=execution_time,
                throughput_rps=throughput_rps,
                memory_before_mb=memory_before,
                memory_after_mb=memory_after,
                memory_used_mb=memory_used,
                memory_per_op_kb=memory_per_op,
                method_used=result.get('method_used', 'a2db_ins'),
                success=True
            )

        except Exception as e:
            return OperationMetrics(
                operation="INSERT",
                batch_size=batch_size,
                rows_affected=0,
                execution_time_ms=0,
                throughput_rps=0,
                memory_before_mb=0,
                memory_after_mb=0,
                memory_used_mb=0,
                memory_per_op_kb=0,
                method_used="error",
                success=False,
                error_message=str(e)
            )

    async def test_select(self, batch_size: int) -> OperationMetrics:
        """Test SELECT operation using a2db_sel()."""
        try:
            query = f"SELECT * FROM {TEST_TABLE} LIMIT {batch_size}"

            gc.collect()
            memory_before = self.get_memory_usage_mb()

            start_time = time.perf_counter()
            result_table = await a2fl.a2db_sel(query)
            execution_time = (time.perf_counter() - start_time) * 1000

            memory_after = self.get_memory_usage_mb()
            memory_used = memory_after - memory_before

            rows_selected = result_table.num_rows
            throughput_rps = rows_selected / (execution_time / 1000) if execution_time > 0 else 0
            memory_per_op = (memory_used * 1024) / max(rows_selected, 1)

            return OperationMetrics(
                operation="SELECT",
                batch_size=batch_size,
                rows_affected=rows_selected,
                execution_time_ms=execution_time,
                throughput_rps=throughput_rps,
                memory_before_mb=memory_before,
                memory_after_mb=memory_after,
                memory_used_mb=memory_used,
                memory_per_op_kb=memory_per_op,
                method_used="a2db_sel",
                success=True
            )

        except Exception as e:
            return OperationMetrics(
                operation="SELECT",
                batch_size=batch_size,
                rows_affected=0,
                execution_time_ms=0,
                throughput_rps=0,
                memory_before_mb=0,
                memory_after_mb=0,
                memory_used_mb=0,
                memory_per_op_kb=0,
                method_used="error",
                success=False,
                error_message=str(e)
            )

    async def test_update(self, batch_size: int) -> OperationMetrics:
        """Test UPDATE operation using a2db_updt()."""
        try:
            query = f"SELECT * FROM {TEST_TABLE} LIMIT {batch_size}"
            update_data = await a2fl.a2db_sel(query)

            if update_data.num_rows == 0:
                raise ValueError("No data available to update")

            # Modify status_code column
            update_data = update_data.set_column(
                update_data.schema.get_field_index('status_code'),
                'status_code',
                pa.array([999] * update_data.num_rows, type=pa.int32())
            )

            gc.collect()
            memory_before = self.get_memory_usage_mb()

            start_time = time.perf_counter()
            result = await a2fl.a2db_updt(update_data, TEST_TABLE)
            execution_time = (time.perf_counter() - start_time) * 1000

            memory_after = self.get_memory_usage_mb()
            memory_used = memory_after - memory_before

            rows_updated = result['rows_affected']
            throughput_rps = rows_updated / (execution_time / 1000) if execution_time > 0 else 0
            memory_per_op = (memory_used * 1024) / max(rows_updated, 1)

            return OperationMetrics(
                operation="UPDATE",
                batch_size=batch_size,
                rows_affected=rows_updated,
                execution_time_ms=execution_time,
                throughput_rps=throughput_rps,
                memory_before_mb=memory_before,
                memory_after_mb=memory_after,
                memory_used_mb=memory_used,
                memory_per_op_kb=memory_per_op,
                method_used=result.get('method_used', 'a2db_updt'),
                success=True
            )

        except Exception as e:
            return OperationMetrics(
                operation="UPDATE",
                batch_size=batch_size,
                rows_affected=0,
                execution_time_ms=0,
                throughput_rps=0,
                memory_before_mb=0,
                memory_after_mb=0,
                memory_used_mb=0,
                memory_per_op_kb=0,
                method_used="error",
                success=False,
                error_message=str(e)
            )

    async def test_delete(self, batch_size: int) -> OperationMetrics:
        """Test DELETE operation using a2db_del()."""
        try:
            query = f"SELECT a2timestamp, device_id FROM {TEST_TABLE} LIMIT {batch_size}"
            delete_data = await a2fl.a2db_sel(query)

            if delete_data.num_rows == 0:
                raise ValueError("No data available to delete")

            gc.collect()
            memory_before = self.get_memory_usage_mb()

            start_time = time.perf_counter()
            result = await a2fl.a2db_del(delete_data, TEST_TABLE)
            execution_time = (time.perf_counter() - start_time) * 1000

            memory_after = self.get_memory_usage_mb()
            memory_used = memory_after - memory_before

            rows_deleted = result['rows_affected']
            throughput_rps = rows_deleted / (execution_time / 1000) if execution_time > 0 else 0
            memory_per_op = (memory_used * 1024) / max(rows_deleted, 1)

            return OperationMetrics(
                operation="DELETE",
                batch_size=batch_size,
                rows_affected=rows_deleted,
                execution_time_ms=execution_time,
                throughput_rps=throughput_rps,
                memory_before_mb=memory_before,
                memory_after_mb=memory_after,
                memory_used_mb=memory_used,
                memory_per_op_kb=memory_per_op,
                method_used=result.get('method_used', 'a2db_del'),
                success=True
            )

        except Exception as e:
            return OperationMetrics(
                operation="DELETE",
                batch_size=batch_size,
                rows_affected=0,
                execution_time_ms=0,
                throughput_rps=0,
                memory_before_mb=0,
                memory_after_mb=0,
                memory_used_mb=0,
                memory_per_op_kb=0,
                method_used="error",
                success=False,
                error_message=str(e)
            )

    def display_operation_results(self, operation: str):
        """Display results for a specific operation with colorful output."""
        op_metrics = [m for m in self.metrics if m.operation == operation]

        if not op_metrics:
            return

        # Operation-specific colors
        op_colors = {
            "INSERT": "bright_blue",
            "SELECT": "bright_green",
            "UPDATE": "bright_yellow",
            "DELETE": "bright_red"
        }

        table = Table(
            title=f"[bold {op_colors.get(operation, 'cyan')}]{operation} Performance Results[/bold {op_colors.get(operation, 'cyan')}]",
            box=box.HEAVY_EDGE,
            show_header=True,
            header_style=f"bold {op_colors.get(operation, 'cyan')}"
        )

        table.add_column("Batch Size", style="bright_cyan", justify="right", no_wrap=True)
        table.add_column("Rows", style="bright_blue", justify="right", no_wrap=True)
        table.add_column("Time (ms)", style="bright_yellow", justify="right", no_wrap=True)
        table.add_column("RPS", style="bright_green bold", justify="right", no_wrap=True)
        table.add_column("Grade", style="bold", justify="center", no_wrap=True)
        table.add_column("Memory/Op", style="bright_magenta", justify="right", no_wrap=True)
        table.add_column("Method", style="white", no_wrap=True)

        for metric in op_metrics:
            if metric.success:
                grade_color = self._get_grade_color(metric.performance_grade)
                mem_color = "bright_green" if metric.meets_memory_target else "bright_yellow"

                table.add_row(
                    f"[bright_cyan]{metric.batch_size:,}[/bright_cyan]",
                    f"[bright_blue]{metric.rows_affected:,}[/bright_blue]",
                    f"[bright_yellow]{metric.execution_time_ms:.1f}[/bright_yellow]",
                    f"[bright_green bold]{metric.throughput_rps:,.0f}[/bright_green bold]",
                    f"[{grade_color} bold]{metric.performance_grade}[/{grade_color} bold]",
                    f"[{mem_color}]{metric.memory_per_op_kb:.1f}KB[/{mem_color}]",
                    f"[white]{metric.method_used}[/white]"
                )
            else:
                table.add_row(
                    f"[dim]{metric.batch_size:,}[/dim]",
                    "[red]0[/red]",
                    "[red bold]FAILED[/red bold]",
                    "[red]0[/red]",
                    "[red bold]F[/red bold]",
                    "[dim]N/A[/dim]",
                    f"[red]Error: {metric.error_message[:30]}[/red]"
                )

        console.print(table)
        console.print()

    def _get_grade_color(self, grade: str) -> str:
        """Get color for performance grade."""
        if grade.startswith('A'):
            return "bright_green"
        elif grade.startswith('B'):
            return "green"
        elif grade.startswith('C'):
            return "yellow"
        elif grade.startswith('D'):
            return "red"
        else:
            return "bright_red"

    def display_summary(self):
        """Display comprehensive summary with vibrant colors."""
        console.print("\n")
        console.print(Panel.fit(
            "[bold bright_white on blue] COMPREHENSIVE PERFORMANCE SUMMARY [/bold bright_white on blue]",
            border_style="bright_blue",
            box=box.DOUBLE
        ))

        total_ops = len([m for m in self.metrics if m.success])
        total_rows = sum(m.rows_affected for m in self.metrics if m.success)
        total_time = sum(m.execution_time_ms for m in self.metrics if m.success) / 1000

        insert_metrics = [m for m in self.metrics if m.operation == "INSERT" and m.success]
        select_metrics = [m for m in self.metrics if m.operation == "SELECT" and m.success]
        update_metrics = [m for m in self.metrics if m.operation == "UPDATE" and m.success]
        delete_metrics = [m for m in self.metrics if m.operation == "DELETE" and m.success]

        summary_table = Table(box=box.HEAVY_HEAD, show_header=False, border_style="bright_cyan")
        summary_table.add_column("Metric", style="bright_cyan bold", width=40)
        summary_table.add_column("Value", style="bright_yellow bold", justify="right")

        summary_table.add_row("[bright_white]Total Operations Executed[/bright_white]", f"[bright_green]{total_ops}[/bright_green]")
        summary_table.add_row("[bright_white]Total Rows Processed[/bright_white]", f"[bright_green]{total_rows:,}[/bright_green]")
        summary_table.add_row("[bright_white]Total Test Duration[/bright_white]", f"[bright_green]{total_time:.2f}s[/bright_green]")
        summary_table.add_row("", "")

        if insert_metrics:
            avg_rps = sum(m.throughput_rps for m in insert_metrics) / len(insert_metrics)
            max_rps = max(m.throughput_rps for m in insert_metrics)
            summary_table.add_row("[bright_blue]INSERT Avg RPS[/bright_blue]", f"[bright_green bold]{avg_rps:,.0f}[/bright_green bold]")
            summary_table.add_row("[bright_blue]INSERT Max RPS[/bright_blue]", f"[bright_green bold]{max_rps:,.0f}[/bright_green bold]")

        if select_metrics:
            avg_rps = sum(m.throughput_rps for m in select_metrics) / len(select_metrics)
            max_rps = max(m.throughput_rps for m in select_metrics)
            summary_table.add_row("[green]SELECT Avg RPS[/green]", f"[bright_green bold]{avg_rps:,.0f}[/bright_green bold]")
            summary_table.add_row("[green]SELECT Max RPS[/green]", f"[bright_green bold]{max_rps:,.0f}[/bright_green bold]")

        if update_metrics:
            avg_rps = sum(m.throughput_rps for m in update_metrics) / len(update_metrics)
            max_rps = max(m.throughput_rps for m in update_metrics)
            summary_table.add_row("[yellow]UPDATE Avg RPS[/yellow]", f"[bright_green bold]{avg_rps:,.0f}[/bright_green bold]")
            summary_table.add_row("[yellow]UPDATE Max RPS[/yellow]", f"[bright_green bold]{max_rps:,.0f}[/bright_green bold]")

        if delete_metrics:
            avg_rps = sum(m.throughput_rps for m in delete_metrics) / len(delete_metrics)
            max_rps = max(m.throughput_rps for m in delete_metrics)
            summary_table.add_row("[red]DELETE Avg RPS[/red]", f"[bright_green bold]{avg_rps:,.0f}[/bright_green bold]")
            summary_table.add_row("[red]DELETE Max RPS[/red]", f"[bright_green bold]{max_rps:,.0f}[/bright_green bold]")

        console.print(summary_table)
        console.print()

    async def run_all_tests(self):
        """Execute comprehensive performance tests."""
        console.print(Panel.fit(
            "[bold bright_white on magenta] A2DB COMPREHENSIVE PERFORMANCE TEST [/bold bright_white on magenta]\n"
            "[bright_cyan]Testing Flight Client CRUD Operations[/bright_cyan]\n"
            f"[bright_yellow]Table: {TEST_TABLE}[/bright_yellow]\n"
            f"[bright_magenta]Methods: a2db_ins, a2db_sel, a2db_updt, a2db_del[/bright_magenta]\n"
            f"[bright_green]Batch Sizes: {', '.join(f'{b:,}' for b in TEST_BATCH_SIZES)}[/bright_green]",
            border_style="bright_magenta",
            box=box.DOUBLE
        ))

        console.print("\n[bold bright_cyan]🚀 Initializing Flight client...[/bold bright_cyan]")

        await self.clear_test_table()

        total_start = time.perf_counter()

        # Operation colors for visual distinction
        op_styles = {
            "INSERT": ("bright_blue", "📥"),
            "SELECT": ("bright_green", "📊"),
            "UPDATE": ("bright_yellow", "✏️"),
            "DELETE": ("bright_red", "🗑️")
        }

        # Run tests for each operation
        for operation, test_func in [
            ("INSERT", self.test_insert),
            ("SELECT", self.test_select),
            ("UPDATE", self.test_update),
            ("DELETE", self.test_delete)
        ]:
            color, emoji = op_styles.get(operation, ("cyan", "•"))

            console.print(f"\n[bold {color}]{'═' * 80}[/bold {color}]")
            console.print(f"[bold {color}]{emoji}  {operation} OPERATIONS  {emoji}[/bold {color}]")
            console.print(f"[bold {color}]{'═' * 80}[/bold {color}]\n")

            with Progress(
                    SpinnerColumn(style=color),
                    TextColumn(f"[{color}]{{task.description}}[/{color}]"),
                    BarColumn(complete_style=color, finished_style=f"bold {color}"),
                    TextColumn(f"[{color}]{{task.percentage:>3.0f}}%[/{color}]"),
                    TimeElapsedColumn(),
                    console=console
            ) as progress:
                task = progress.add_task(f"{operation} tests", total=len(TEST_BATCH_SIZES))

                for batch_size in TEST_BATCH_SIZES:
                    progress.update(task, description=f"{operation} {batch_size:,} rows")
                    metric = await test_func(batch_size)
                    self.metrics.append(metric)

                    if metric.success:
                        grade_color = self._get_grade_color(metric.performance_grade)
                        console.print(
                            f"[bold green]✓[/bold green] [{color}]{batch_size:,:>10}[/{color}]: "
                            f"[bright_green bold]{metric.throughput_rps:>12,.0f}[/bright_green bold] RPS, "
                            f"Grade [{grade_color} bold]{metric.performance_grade:>3}[/{grade_color} bold]"
                        )
                    else:
                        console.print(f"[bold red]✗ {batch_size:,}: {metric.error_message}[/bold red]")

                    progress.advance(task)

            self.display_operation_results(operation)

        total_time = time.perf_counter() - total_start
        self.display_summary()

        # Close global client
        await a2fl.a2db_close()

        success_rate = len([m for m in self.metrics if m.success]) / len(self.metrics) * 100

        console.print(Panel.fit(
            f"[bold bright_white on green] ALL TESTS COMPLETED [/bold bright_white on green]\n\n"
            f"[bright_cyan]⏱️  Total Duration:[/bright_cyan] [bright_yellow bold]{total_time:.2f}s[/bright_yellow bold]\n"
            f"[bright_cyan]📊 Operations:[/bright_cyan] [bright_yellow bold]{len(self.metrics)}[/bright_yellow bold]\n"
            f"[bright_cyan]✅ Success Rate:[/bright_cyan] [bright_green bold]{success_rate:.1f}%[/bright_green bold]",
            border_style="bright_green",
            box=box.DOUBLE
        ))


async def main():
    """Main entry point."""
    runner = PerformanceTestRunner()
    await runner.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())