Fixed issue during shutdown or restart of a sink with disk buffer configured where
component would stall for batch.timeout_sec before fully gracefully shutting down.

authors: graphcareful
