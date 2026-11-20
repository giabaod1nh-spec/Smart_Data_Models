"""V1 worker and health server entry point."""
import logging
import threading
from pathlib import Path
import clickhouse_connect
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from de.kafka_raw.v1_config import V1RawSettings
from de.kafka_raw.v1_ledger_store import V1LedgerStore
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
from de.kafka_raw.v1_consumer import V1RawKafkaConsumer

def main():
    logging.basicConfig(level=logging.INFO)
    settings=V1RawSettings()
    # OS lock is held for the lifetime of the supported single local owner.
    lock_path=Path(settings.ledger_path+".lock")
    lock_path.parent.mkdir(parents=True,exist_ok=True)
    lock=lock_path.open("a+b")
    if __import__("os").name=="nt":
        import msvcrt
        lock.seek(0)
        lock.write(b"0")
        lock.flush()
        lock.seek(0)
        msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
    else:
        import fcntl
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    client=clickhouse_connect.get_client(host=settings.clickhouse_host,
        port=settings.clickhouse_port,database="smart_traffic",
        connect_timeout=3,send_receive_timeout=10,autogenerate_session_id=False)
    ledger=V1LedgerStore(settings.ledger_path)
    worker=V1RawKafkaConsumer(settings,V1ClickHouseRepository(client),ledger)
    thread=threading.Thread(target=worker.run,daemon=True)
    thread.start()
    app=FastAPI()

    @app.get("/health")
    def health():
        return dict(worker.health(),consumer_thread_alive=thread.is_alive())

    @app.get("/ready")
    def ready():
        h=health()
        return JSONResponse(h,status_code=200 if h["ready"] and thread.is_alive() else 503)

    @app.get("/metrics")
    def metrics():
        return health()

    @app.get("/cutover-ready")
    def cutover_ready():
        h=health()
        offsets=h.get("partition_offsets",[])
        caught_up=bool(offsets) and all(p.get("lag")==0 for p in offsets)
        return JSONResponse(h,status_code=200 if h["ready"] and caught_up and not worker.pending else 503)

    @app.on_event("shutdown")
    def shutdown():
        worker.running=False
        thread.join(15)
        if not thread.is_alive():
            ledger.close()
            client.close()
    uvicorn.run(app,host="0.0.0.0",port=settings.health_port)

if __name__=="__main__":
    main()

