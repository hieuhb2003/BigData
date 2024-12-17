FROM python:3.10-slim

WORKDIR /app

COPY ./visualize/requirements.txt .
RUN pip install -r requirements.txt

COPY ./src/visualize/dashboard.py .

EXPOSE 8501

RUN mkdir ~/.streamlit

RUN echo "[global]\nshowWarningOnDirectExecution = true\n\n[logger]\nlevel = \"debug\"\n\n[runner]\nmagicEnabled = true\n\n[server]\nfolderWatchBlacklist = ['']\nheadless = true\nliveSave = false\nrunOnSave = false\nport = 8501\nenableCORS = false\n\n[browser]\nserverAddress = \"0.0.0.0\"\ngatherUsageStats = true\nserverPort = 8501" > ~/.streamlit/config.toml

RUN echo "[general]\nemail=\"\"" > ~/.streamlit/credentials.toml

ENV POSTGRES_HOST=host.docker.internal \
    POSTGRES_PORT=5432 \
    POSTGRES_DB=reddit-data \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres

ENTRYPOINT ["streamlit", "run"]

CMD ["dashboard.py"]