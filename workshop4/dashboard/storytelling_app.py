import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Data Loading ─────────────────────────────────────────────────────────────
# Gold layer location for storytelling outputs.
GOLD_PATH = Path("../datalake_gold")

# Load the newest parquet file (by modification time) matching a pattern.
def load_latest(pattern):
    files = list(GOLD_PATH.glob(pattern))
    if not files:
        return pd.DataFrame()
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return pd.read_parquet(latest_file)

# Load the latest storytelling datasets from Gold.
df_volume = load_latest("storytelling_volume_*.parquet")
df_keywords = load_latest("storytelling_keywords_*.parquet")
df_score = load_latest("storytelling_score_trend_*.parquet")
df_authors = load_latest("storytelling_authors_*.parquet")
df_sentiment_dist = load_latest("storytelling_sentiment_dist_*.parquet")
df_sentiment_trend = load_latest("storytelling_sentiment_trend_*.parquet")

# Timestamp for the header (runtime, not data time).
last_updated = datetime.now().strftime("%Y-%m-%d %H:%M")

# ── App Setup ─────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="Storytelling Dashboard"
)

# Color palette for sentiment labels.
SENTIMENT_COLORS = {
    "positive": "#2ecc71",
    "negative": "#e74c3c",
    "neutral": "#95a5a6"
}

# Color palette for sources.
SOURCE_COLORS = {
    "reddit": "#FF6B6B",
    "lasillavacia": "#4ECDC4"
}

# ── Layout ────────────────────────────────────────────────────────────────────
# Compose narrative sections and visual insights for the storytelling view.
app.layout = dbc.Container([

    # Header
    dbc.Row([
        dbc.Col([
            html.H2("🇨🇴 Public Opinion on Colombian Politics", className="fw-bold text-primary mt-3"),
            html.P("Sentiment Analysis — Reddit r/Colombia + La Silla Vacía Opinion Section",
                   className="text-muted"),
            html.Small(f"Last updated: {last_updated}", className="text-muted")
        ])
    ], className="mb-3"),

    html.Hr(),

    # Narrative Card
    dbc.Row([
        dbc.Col([
            dbc.Alert([
                html.H5("📌 Key Insight", className="alert-heading fw-bold"),
                html.P(
                    "Colombian political discourse online is predominantly neutral, "
                    "with negative sentiment significantly outweighing positive sentiment. "
                    "Topics related to corruption, economic crisis, and government performance "
                    "dominate public conversation, while keywords like 'petro', 'gobierno', "
                    "and 'reforma' appear most frequently across both sources.",
                    className="mb-0"
                )
            ], color="info")
        ])
    ], className="mb-4"),

    # Overall Sentiment Donut Chart (NEW)
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="overall-sentiment-donut")
        ], md=12)
    ], className="mb-4"),

    # Sentiment Distribution
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="sentiment-dist-chart")
        ], md=6),
        dbc.Col([
            dcc.Graph(id="sentiment-trend-chart")
        ], md=6),
    ], className="mb-4"),

    # Volume and Keywords
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="volume-chart")
        ], md=6),
        dbc.Col([
            dcc.Graph(id="keywords-chart")
        ], md=6),
    ], className="mb-4"),

    # Source Comparison and Score Trend
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="source-comparison-chart")
        ], md=6),
        dbc.Col([
            dcc.Graph(id="score-trend-chart")
        ], md=6),
    ], className="mb-4"),

], fluid=True)

# ── Callbacks ─────────────────────────────────────────────────────────────────

# NEW: Overall sentiment donut chart
@app.callback(
    dash.Output("overall-sentiment-donut", "figure"),
    dash.Input("overall-sentiment-donut", "id")
)
def update_overall_donut(_):
    if df_sentiment_dist.empty:
        return go.Figure()
    
    # Aggregate counts across all sources
    overall_counts = df_sentiment_dist.groupby("sentiment")["count"].sum().reset_index()
    
    # Normalize sentiment strings
    overall_counts["sentiment"] = overall_counts["sentiment"].astype(str).str.strip().str.lower()
    
    # Filter only valid sentiments
    overall_counts = overall_counts[overall_counts["sentiment"].isin(SENTIMENT_COLORS)]
    
    if overall_counts.empty:
        return go.Figure()
    
    # Create donut chart
    fig = go.Figure(data=[go.Pie(
        labels=overall_counts["sentiment"],
        values=overall_counts["count"],
        hole=0.4,  # This creates the donut effect
        marker_colors=[SENTIMENT_COLORS[s] for s in overall_counts["sentiment"]],
        textinfo="label+percent",
        textposition="auto",
        hoverinfo="label+value+percent",
        pull=[0.05, 0, 0]  # Slightly pull the largest slice if needed
    )])
    
    fig.update_layout(
        title={
            'text': "🎯 Overall Sentiment Breakdown (All Sources)",
            'x': 0.5,
            'xanchor': 'center'
        },
        annotations=[dict(
            text=f"Total: {overall_counts['count'].sum():,}",
            x=0.5, y=0.5,
            font_size=14,
            showarrow=False
        )],
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=450
    )
    
    return fig

# Sentiment distribution by source (bar facets).
@app.callback(
    dash.Output("sentiment-dist-chart", "figure"),
    dash.Input("sentiment-dist-chart", "id")
)
def update_sentiment_dist(_):
    if df_sentiment_dist.empty:
        return go.Figure()
    fig = px.bar(
        df_sentiment_dist, x="sentiment", y="count", color="sentiment",
        facet_col="source",
        title="Sentiment Distribution by Source",
        color_discrete_map=SENTIMENT_COLORS,
        labels={"count": "Number of Records", "sentiment": "Sentiment"}
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", showlegend=False)
    return fig

# Sentiment trend over time by source (line chart).
@app.callback(
    dash.Output("sentiment-trend-chart", "figure"),
    dash.Input("sentiment-trend-chart", "id")
)
def update_sentiment_trend(_):
    if df_sentiment_trend.empty:
        return go.Figure()
    fig = px.line(
        df_sentiment_trend, x="fecha", y="avg_polarity", color="source",
        title="Sentiment Trend Over Time",
        color_discrete_map=SOURCE_COLORS,
        labels={"avg_polarity": "Average Polarity", "fecha": "Date", "source": "Source"}
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Neutral")
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    return fig

# Publication volume over time by source.
@app.callback(
    dash.Output("volume-chart", "figure"),
    dash.Input("volume-chart", "id")
)
def update_volume(_):
    if df_volume.empty:
        return go.Figure()
    fig = px.bar(
        df_volume, x="fecha", y="record_count", color="source",
        title="Publication Volume Over Time",
        color_discrete_map=SOURCE_COLORS,
        labels={"record_count": "Number of Records", "fecha": "Date", "source": "Source"}
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    return fig

# Top 20 keywords across sources.
@app.callback(
    dash.Output("keywords-chart", "figure"),
    dash.Input("keywords-chart", "id")
)
def update_keywords(_):
    if df_keywords.empty:
        return go.Figure()
    top20 = df_keywords.nlargest(20, "frequency")
    fig = px.bar(
        top20, x="frequency", y="word", color="source",
        orientation="h",
        title="Top 20 Keywords in Political Discourse",
        color_discrete_map=SOURCE_COLORS,
        labels={"frequency": "Frequency", "word": "Keyword", "source": "Source"}
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", yaxis={"categoryorder": "total ascending"})
    return fig

# Stacked sentiment share by source (percentage).
@app.callback(
    dash.Output("source-comparison-chart", "figure"),
    dash.Input("source-comparison-chart", "id")
)
def update_source_comparison(_):
    if df_sentiment_dist.empty:
        return go.Figure()
    total_by_source = df_sentiment_dist.groupby("source")["count"].sum().reset_index()
    sentiment_pct = df_sentiment_dist.merge(total_by_source, on="source", suffixes=("", "_total"))
    sentiment_pct["percentage"] = (sentiment_pct["count"] / sentiment_pct["count_total"] * 100).round(2)
    sentiment_pct = sentiment_pct.copy()
    sentiment_pct["sentiment"] = (
        sentiment_pct["sentiment"].astype(str).str.strip().str.lower()
    )
    sentiment_pct = sentiment_pct[sentiment_pct["sentiment"].isin(SENTIMENT_COLORS)]
    if sentiment_pct.empty:
        return go.Figure()
    fig = go.Figure()
    for sentiment in ("positive", "neutral", "negative"):
        subset = sentiment_pct[sentiment_pct["sentiment"] == sentiment]
        if subset.empty:
            continue
        fig.add_bar(
            x=subset["source"],
            y=subset["percentage"],
            name=sentiment,
            marker_color=SENTIMENT_COLORS[sentiment]
        )
    fig.update_layout(
        title="Sentiment Distribution by Source (%)",
        xaxis_title="Source",
        yaxis_title="Percentage (%)",
        barmode="relative"
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    return fig

# Reddit score trend over time.
@app.callback(
    dash.Output("score-trend-chart", "figure"),
    dash.Input("score-trend-chart", "id")
)
def update_score_trend(_):
    if df_score.empty:
        return go.Figure()
    fig = px.line(
        df_score, x="fecha", y="avg_score",
        title="Reddit Average Score Over Time",
        labels={"avg_score": "Average Score", "fecha": "Date"},
        color_discrete_sequence=["#FF6B6B"]
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    return fig

# Local dev entrypoint.
if __name__ == "__main__":
    app.run(debug=True, port=8051)