"""JDL_system package: Centralized and distributed query execution over a bounded BUS.

Modules:
- bus: bounded queue measured in triples
- data_generator: synthetic triple generator
- queries: q1 and q2 definitions
- centralized: sequential execution using BUS
- distributed: threaded execution using BUS
- utils: timing and memory measurement helpers
- experiment: runner to compare modes on 60k triples
"""

__all__ = [
	"bus",
	"data_generator",
	"queries",
	"centralized",
	"distributed",
	"utils",
	"experiment",
]


