from envoy_recorder.envoy_recorder import EnvoyRecorder
from envoy_recorder.logging import get_logger

log = get_logger(__name__)

if __name__ == "__main__":
    log.info("Starting up!")
    try:
        envoy_recorder = EnvoyRecorder()
        envoy_recorder.run()
    except:
        log.exception("Exception raised in main()!")
        raise
    log.info("Finished!\n")
