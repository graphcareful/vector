Fixed a durability gap in the `disk_v2` buffer that could wedge it after a crash during a data-file roll. Advancing to a new data file was not persisted to the ledger until the next periodic flush, so a crash in between could leave the new file on disk while the durable ledger still pointed at the previous one — a desync that deadlocked the buffer on restart. The ledger is now flushed as soon as the writer rolls to a new file.

authors: graphcareful
