# quartercall

Read QuarterCall Data — the NCUA Form 5300 Call Reports, republished as Parquet at
`data.quartercall.com` — into pandas. No account or API key is needed.

```sh
pip install quartercall
```

```python
import quartercall

quartercall.quarters()
```

`quarters()` returns one row per published quarter, newest first:

| Column | Holds |
|---|---|
| `quarter` | the cycle, such as `2026Q3` |
| `cycle_date` | the cycle's last day |
| `status` | `open` while filings may still arrive; `dormant` once NCUA has validated none for over a year |
| `cu_count` | credit unions that have filed for the cycle |
| `updated_at` | when the quarter's files were last rebuilt, in UTC |

No quarter is final: NCUA accepts an amendment to a cycle of any age, and a `dormant` quarter
becomes `open` again when one arrives. A recent quarter is published while many credit unions
have yet to file.

Version 0.0.1 lists the quarters. Reading a quarter's accounts and profiles comes next.

QuarterCall is a registered trademark of QuarterCall Holdings Inc. The code is licensed under the
Apache License 2.0.
