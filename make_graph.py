import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def Make_graph(path: str):
    df = pd.read_csv(path)

    df['sale_price'] = df['sale_price'].replace({',': '', '\$': ''}, regex=True).astype(float)

    # Remove outliers via IQR
    # Cant believe Im using ts outside of school
    Q1 = df['sale_price'].quantile(0.2)
    Q3 = df['sale_price'].quantile(0.7)
    IQR = Q3 - Q1
    df = df[(df['sale_price'] >= Q1 - 1.5 * IQR) & (df['sale_price'] <= Q3 + 1.5 * IQR)]

    # Avg sale price per day
    daily_avg_price = df.groupby('sale_date')['sale_price'].mean().reset_index()

    # Calculate $/sqft and average it per day (only for rows with valid sqft)
    df_valid_sqft = df[df['fin_sqft'].notnull() & (df['fin_sqft'] > 0)].copy()
    df_valid_sqft['price_per_sqft'] = df_valid_sqft['sale_price'] / df_valid_sqft['fin_sqft']
    daily_avg_ppsqft = df_valid_sqft.groupby('sale_date')['price_per_sqft'].mean().reset_index()

    # Create subplot with 2 rows, 1 column, shared x-axis optional
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("Average Sale Price Per Day", "Average $/Sqft Sold Per Day")
    )

    fig.add_trace(go.Scatter(
        x=daily_avg_price['sale_date'],
        y=daily_avg_price['sale_price'],
        mode='lines+markers',
        name='Avg Sale Price',
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=daily_avg_ppsqft['sale_date'],
        y=daily_avg_ppsqft['price_per_sqft'],
        mode='lines+markers',
        name='Avg $/Sqft',
        marker_color='orange'
    ), row=2, col=1)

    fig.update_layout(
        width=1600,
        height=900,
        plot_bgcolor='rgb(230,230,230)',
        showlegend=True,
    )

    fig.update_xaxes(title_text="Sale Date", row=2, col=1)
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Price per Sqft (USD)", row=2, col=1)

    fig.write_image(path.replace(".csv", ".png"), width=1600, height=1000)

if __name__ == "__main__":
    Make_graph("output/ALL_SOLD_04-14-2025_to_08-12-2025.csv")