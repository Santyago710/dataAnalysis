import dash
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Data Loading ────────────────────────────────────────────────────────────
# Gold layer location for governance KPIs.
GOLD_PATH = Path("../datalake_gold")

# Load the newest governance parquet file (by modification time).
def load_latest_governance():
    files = list(GOLD_PATH.glob("governance_*.parquet"))
    if not files:
        return pd.DataFrame()
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return pd.read_parquet(latest_file)

# Cache data and timestamp for the dashboard header.
df = load_latest_governance()
last_updated = datetime.now().strftime("%Y-%m-%d %H:%M")

# ── App Setup ────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="Governance Dashboard"
)

# ── Helper: KPI Card ─────────────────────────────────────────────────────────
# Build a consistent Bootstrap card for top-level metrics.
def kpi_card(title, value, color="primary"):
    return dbc.Card(
        dbc.CardBody([
            html.H6(title, className="card-subtitle text-muted mb-1"),
            html.H3(value, className=f"card-title text-{color} fw-bold")
        ]),
        className="shadow-sm h-100"
    )

# ── Layout ───────────────────────────────────────────────────────────────────
# Compose the dashboard sections: header, KPIs, charts, and a data table.
app.layout = dbc.Container([

    # Header
    dbc.Row([
        dbc.Col([
            html.H2("📊 Data Governance Dashboard", className="fw-bold text-primary mt-3"),
            html.P("Public Opinion & Sentiment Analysis — Colombia Political Discourse", className="text-muted"),
            html.Small(f"Last updated: {last_updated} | Sources: Reddit r/Colombia + La Silla Vacía",
                      className="text-muted")
        ])
    ], className="mb-3"),

    html.Hr(),

    # KPI Cards
    dbc.Row([
        dbc.Col(kpi_card("Total Records Reddit",
            str(int(df[df["source"]=="reddit"]["total_records"].values[0])) if not df.empty else "N/A",
            "primary"), md=3),
        dbc.Col(kpi_card("Total Records La Silla Vacía",
            str(int(df[df["source"]=="lasillavacia"]["total_records"].values[0])) if not df.empty else "N/A",
            "success"), md=3),
        dbc.Col(kpi_card("Avg Null Rate Reddit",
            f"{df[df['source']=='reddit'][['null_rate_title','null_rate_author','null_rate_text']].mean(axis=1).values[0]:.2f}%" if not df.empty else "N/A",
            "warning"), md=3),
        dbc.Col(kpi_card("Avg Null Rate La Silla Vacía",
            f"{df[df['source']=='lasillavacia'][['null_rate_title','null_rate_author','null_rate_text']].mean(axis=1).values[0]:.2f}%" if not df.empty else "N/A",
            "info"), md=3),
    ], className="mb-4"),

    # Null Rate Charts
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="null-rate-chart")
        ], md=6),
        dbc.Col([
            dcc.Graph(id="duplicate-rate-chart")
        ], md=6),
    ], className="mb-4"),

    # Text Length Stats
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="text-length-chart")
        ], md=6),
        dbc.Col([
            dcc.Graph(id="score-stats-chart")
        ], md=6),
    ], className="mb-4"),

    # Data Quality Table
    dbc.Row([
        dbc.Col([
            html.H5("📋 Full Governance KPI Summary", className="fw-bold mb-3"),
            dash_table.DataTable(
                data=df.to_dict("records") if not df.empty else [],
                columns=[{"name": c, "id": c} for c in df.columns] if not df.empty else [],
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "8px", "fontSize": "13px"},
                style_header={"backgroundColor": "#2c3e50", "color": "white", "fontWeight": "bold"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#f8f9fa"}
                ]
            )
        ])
    ], className="mb-4"),

], fluid=True)

# ── Callbacks ────────────────────────────────────────────────────────────────
# Render a grouped bar chart of null rates by field and source.
@app.callback(
    dash.Output("null-rate-chart", "figure"),
    dash.Input("null-rate-chart", "id")
)
def update_null_rate(_):
    if df.empty:
        return go.Figure()
    melted = df.melt(
        id_vars=["source"],
        value_vars=["null_rate_title", "null_rate_author", "null_rate_text", "null_rate_url"],
        var_name="field", value_name="null_rate"
    )
    melted["field"] = melted["field"].str.replace("null_rate_", "")
    fig = go.Figure()
    color_map = {"reddit": "#FF6B6B", "lasillavacia": "#4ECDC4"}
    for source, source_df in melted.groupby("source", sort=False):
        fig.add_trace(go.Bar(
            name=source,
            x=source_df["null_rate"],
            y=source_df["field"],
            orientation="h",
            marker_color=color_map.get(source, "#4C78A8"),
        ))
    fig.update_layout(
        title="Null Rate per Field (%)",
        barmode="group",
        xaxis_title="Null Rate (%)",
        yaxis_title="Field",
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    return fig

# Render duplicate rate per data source.
@app.callback(
    dash.Output("duplicate-rate-chart", "figure"),
    dash.Input("duplicate-rate-chart", "id")
)
def update_duplicate_rate(_):
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    color_map = {"reddit": "#FF6B6B", "lasillavacia": "#4ECDC4"}
    for _, row in df.iterrows():
        fig.add_trace(go.Bar(
            name=row["source"],
            x=[row["source"]],
            y=[row["duplicate_rate"]],
            marker_color=color_map.get(row["source"], "#4C78A8"),
        ))
    fig.update_layout(
        title="Duplicate Rate per Source (%)",
        xaxis_title="Source",
        yaxis_title="Duplicate Rate (%)",
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
    )
    return fig

# Render title/content length stats for each source.
@app.callback(
    dash.Output("text-length-chart", "figure"),
    dash.Input("text-length-chart", "id")
)
def update_text_length(_):
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    for _, row in df.iterrows():
        fig.add_trace(go.Bar(
            name=row["source"],
            x=["Min", "Mean", "Max"],
            y=[row["title_len_min"], row["title_len_mean"], row["title_len_max"]]
        ))
    fig.update_layout(
        title="Title/Content Length Statistics (chars)",
        barmode="group",
        plot_bgcolor="white",
        paper_bgcolor="white"
    )
    return fig

# Render Reddit score stats (min/mean/median/max).
@app.callback(
    dash.Output("score-stats-chart", "figure"),
    dash.Input("score-stats-chart", "id")
)
def update_score_stats(_):
    reddit_df = df[df["source"] == "reddit"]
    if reddit_df.empty:
        return go.Figure()
    row = reddit_df.iloc[0]
    fig = go.Figure(go.Bar(
        x=["Min", "Mean", "Median", "Max"],
        y=[row["score_min"], row["score_mean"], row["score_median"], row["score_max"]],
        marker_color=["#FF6B6B", "#FF6B6B", "#FF6B6B", "#FF6B6B"]
    ))
    fig.update_layout(
        title="Reddit Score Statistics",
        plot_bgcolor="white",
        paper_bgcolor="white",
        yaxis_title="Score"
    )
    return fig

# Local dev entrypoint.
if __name__ == "__main__":
    app.run(debug=True, port=8050)
