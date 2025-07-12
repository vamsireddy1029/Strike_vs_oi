import streamlit as st
import pandas as pd
import sqlite3
import re
import datetime
import plotly.graph_objects as go
from collections import defaultdict
import numpy as np

st.set_page_config(layout="wide")
st.title("📊 OI Change Visualizer")

# Load snapshot data

def load_data(path):
    conn = sqlite3.connect(path)
    df = pd.read_sql_query("SELECT snapshot_time as timestamp, trading_symbol, oi FROM oi_snapshot", conn)
    conn.close()
    return df

db_path = "snapshot_data.db"
df = load_data(db_path)

# Clean timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
df = df.dropna(subset=['timestamp', 'trading_symbol', 'oi'])
df['oi'] = pd.to_numeric(df['oi'], errors='coerce')
df = df.dropna(subset=['oi'])

# Extract symbol, expiry, strike, type
def extract_parts(symbol):
    # Determine option type
    opt_type = 'CE' if 'CE' in symbol else 'PE' if 'PE' in symbol else 'FUT'
    clean_symbol = symbol.replace('CE', '').replace('PE', '').replace('FUT', '')

    # Case 1: MDD format (like SENSEX2580588500 → SYMBOL + YY + MDD + STRIKE)
    match_mdd_strike = re.match(r'^([A-Z]+)(\d{2})(\d)(\d{2})(\d+)$', clean_symbol)
    if match_mdd_strike:
        root = match_mdd_strike.group(1)        # e.g., SENSEX
        # year = match_mdd_strike.group(2)      # optional
        month = int(match_mdd_strike.group(3))  # M
        day = int(match_mdd_strike.group(4))    # DD
        strike = int(match_mdd_strike.group(5)) # Strike
        expiry = f"{day:02d}-{month:02d}"       # Format: DD-MM
        return root, expiry, strike, opt_type

    # Case 2: BANKEX25AUG65500 → SYMBOL + DD + MMM + STRIKE
    match_ddmmm_strike = re.match(r'^([A-Z]+)(\d{2})([A-Z]{3})(\d+)$', clean_symbol)
    if match_ddmmm_strike:
        root = match_ddmmm_strike.group(1)
        day = int(match_ddmmm_strike.group(2))
        month_str = match_ddmmm_strike.group(3).title()
        strike = int(match_ddmmm_strike.group(4))
        expiry = f"{day:02d}-{month_str}"
        return root, expiry, strike, opt_type

    # Case 3: CRUDEOILM25JUL → SYMBOL + DD + MMM (no strike)
    match_ddmmm = re.match(r'^([A-Z]+)(\d{2})([A-Z]{3})$', clean_symbol)
    if match_ddmmm:
        root = match_ddmmm.group(1)
        day = int(match_ddmmm.group(2))
        month_str = match_ddmmm.group(3).title()
        expiry = f"{day:02d}-{month_str}"
        return root, expiry, None, opt_type

    # Default return if no match
    return None, None, None, opt_type

df[['symbol', 'expiry', 'strike', 'type']] = df['trading_symbol'].apply(lambda x: pd.Series(extract_parts(x)))
df = df.dropna(subset=['symbol', 'expiry', 'strike'])

# Filters
symbols = sorted(df['symbol'].unique())

col1, col2 = st.columns([1, 1])
with col1:
    selected_symbol = st.selectbox("Select Symbol", symbols)
expiries = sorted(
    df[df['symbol'] == selected_symbol]['expiry'].dropna().unique(),
    key=lambda x: pd.to_datetime(x, format='%d-%m') if re.match(r'\d{2}-\d{2}', x) else pd.to_datetime(x, format='%d-%b')
)
with col2:
    selected_expiry = st.selectbox("Select Expiry", expiries)


filtered = df[(df['symbol'] == selected_symbol) & (df['expiry'] == selected_expiry)]
filtered['rounded_time'] = filtered['timestamp'].dt.floor('3min')
filtered = filtered.dropna(subset=['rounded_time'])

available_times = sorted(filtered['rounded_time'].dt.time.unique())
if not available_times:
    st.error("No available times in the data for selected symbol/expiry.")
    st.stop()

min_time = available_times[0]
max_time = available_times[-1]

# Time Range Slider
t1, t2 = st.slider(
    "Select Time Range (T1 and T2)",
    min_value=min_time, max_value=max_time,
    value=(min_time, max_time),
    step=datetime.timedelta(minutes=3),
    format="HH:mm"
)

# Map: time → {trading_symbol → oi}
time_oi_map = defaultdict(dict)
for row in filtered.itertuples():
    time = row.rounded_time.time()
    time_oi_map[time][row.trading_symbol] = row.oi

def find_nearest_time(time_dict, target, find_min=True):
    time_keys = sorted(time_dict.keys())
    for t in time_keys if find_min else reversed(time_keys):
        if (t >= target if find_min else t <= target):
            return t
    return None

t1_key = find_nearest_time(time_oi_map, t1, find_min=True)
t2_key = find_nearest_time(time_oi_map, t2, find_min=False)

if not t1_key or not t2_key:
    st.warning("No data available for selected time range.")
    st.stop()

t1_data = time_oi_map[t1_key]
t2_data = time_oi_map[t2_key]
# Collect all strikes available in t1 and t2 data
strike_set = set()
for symbol in set(t1_data.keys()).union(set(t2_data.keys())):
    _, _, strike, opt_type = extract_parts(symbol)
    if strike is not None:
        strike_set.add(strike)

if not strike_set:
    st.warning("No strike values found in the selected time range.")
    st.stop()

min_strike = int(min(strike_set))
max_strike = int(max(strike_set))

# Add Strike Range Slider
st1, st2 = st.slider(
    "Select Strike Range",
    min_value=min_strike,
    max_value=max_strike,
    value=(min_strike, max_strike),
    step=100
)

# Build comparison: {(strike, type): (t1_oi, t2_oi)}
# Build comparison: {(strike, type): (t1_oi, t2_oi)}
comparison = {}
all_symbols = set(t1_data.keys()).union(set(t2_data.keys()))
for symbol in all_symbols:
    try:
        _, _, strike, opt_type = extract_parts(symbol)

        # ✅ Filter based on selected strike range
        if strike is not None and st1 <= strike <= st2:
            t1_oi = t1_data.get(symbol, 0)
            t2_oi = t2_data.get(symbol, 0)
            comparison[(strike, opt_type)] = (t1_oi, t2_oi)
    except:
        continue

# ✅ Optional: Show info message if only one strike is selected
if st1 == st2:
    st.info(f"Only one strike ({st1}) selected. Try widening the range for a better view.")


# Process data according to your exact conditions
def process_oi_data(comparison_data):
    """Process data according to exact conditions specified"""
    chart_data = []
    
    for (strike, opt_type), (t1_oi, t2_oi) in comparison_data.items():
        chart_data.append({
            'strike': strike,
            'type': opt_type,
            't1_oi': t1_oi,
            't2_oi': t2_oi,
            'condition': 'decrease' if t1_oi >= t2_oi else 'increase'
        })
    
    return sorted(chart_data, key=lambda x: x['strike'])

chart_data = process_oi_data(comparison)

# Create Chart with exact conditions
fig = go.Figure()

# Color mapping
colors = {'CE': '#22c55e', 'PE': '#ef4444'}  # Green for CE, Red for PE

# Track legend items to avoid duplicates
legend_added = {
    'call_filled': False,
    'put_filled': False,
    'call_striped': False,
    'put_striped': False,
    'call_hollow': False,
    'put_hollow': False
}

for item in chart_data:
    strike = item['strike']
    opt_type = item['type']
    t1_oi = item['t1_oi']
    t2_oi = item['t2_oi']
    color = colors[opt_type]
    
    # Position for CE/PE side by side
    x_pos = f"{strike}_{opt_type}"
    
    # CONDITION 1: t1_oi >= t2_oi (OI Decrease)
    if t1_oi >= t2_oi and t1_oi != 0 and t2_oi != 0:
        # 1. Generate empty bar of t1_oi size (hollow with colored border)
        fig.add_trace(go.Bar(
            x=[x_pos],
            y=[t1_oi],
            name=f"{'Call' if opt_type == 'CE' else 'Put'} Decrease",
            marker=dict(
                color='rgba(0,0,0,0)',  # Transparent fill
                line=dict(color=color, width=2)  # Colored border
            ),
            showlegend=not legend_added[f"{'call' if opt_type == 'CE' else 'put'}_hollow"],
            hovertemplate=f"<b>{strike} {opt_type}</b><br>" +
                         f"T1: {t1_oi}<br>" +
                         f"T2: {t2_oi}<br>" +
                         f"Decrease: -{t1_oi - t2_oi}<br>" +
                         "<extra></extra>"
        ))
        legend_added[f"{'call' if opt_type == 'CE' else 'put'}_hollow"] = True
        
        # 2. Fill the empty bar till t2_oi with striped pattern
        if t2_oi > 0:
            fig.add_trace(go.Bar(
            x=[x_pos],
            y=[t2_oi],
            name=f"{'Call' if opt_type == 'CE' else 'Put'} OI",
            marker_color=color,
            showlegend=not legend_added[f"{'call' if opt_type == 'CE' else 'put'}_filled"],
            hovertemplate=f"<b>{strike} {opt_type}</b><br>" +
                         f"T1: {t1_oi}<br>" +
                         f"T2: {t2_oi}<br>" +
                         f"Base OI: {t2_oi}<br>" +
                         "<extra></extra>"
        ))
        legend_added[f"{'call' if opt_type == 'CE' else 'put'}_filled"] = True
    
    # CONDITION 2: t1_oi < t2_oi (OI Increase)
    else:
        # 1. Generate completely filled bar of t2_oi size
        fig.add_trace(go.Bar(
            x=[x_pos],
            y=[t1_oi],
            name=f"{'Call' if opt_type == 'CE' else 'Put'} OI",
            marker_color=color,
            showlegend=not legend_added[f"{'call' if opt_type == 'CE' else 'put'}_filled"],
            hovertemplate=f"<b>{strike} {opt_type}</b><br>" +
                         f"T1: {t1_oi}<br>" +
                         f"T2: {t2_oi}<br>" +
                         f"Base OI: {t2_oi}<br>" +
                         "<extra></extra>"
        ))
        legend_added[f"{'call' if opt_type == 'CE' else 'put'}_filled"] = True
        
        # 2. Add the difference (t2_oi - t1_oi) on top with striped pattern
        diff = t2_oi - t1_oi
        if diff > 0:
            fig.add_trace(go.Bar(
                x=[x_pos],
                y=[diff],
                base=[t1_oi],  # Stack on top of the filled bar
                name=f"{'Call' if opt_type == 'CE' else 'Put'} Increase",
                marker=dict(
                    color=color,
                    pattern=dict(
                        shape="/",  # Diagonal stripes
                        bgcolor="rgba(255,255,255,0.3)",
                        fgcolor=color,
                        size=6
                    )
                ),
                showlegend=not legend_added[f"{'call' if opt_type == 'CE' else 'put'}_striped"],
                hovertemplate=f"<b>{strike} {opt_type}</b><br>" +
                             f"Increase: +{diff}<br>" +
                             f"T1: {t1_oi} → T2: {t2_oi}<br>" +
                             "<extra></extra>"
            ))
            legend_added[f"{'call' if opt_type == 'CE' else 'put'}_striped"] = True

chart_data = process_oi_data(comparison)

# Update layout
fig.update_layout(
    title=f"<b>{selected_symbol} OI Change Analysis</b><br>" +
          f"<sub>From {t1_key.strftime('%H:%M')} to {t2_key.strftime('%H:%M')} on {selected_expiry}</sub>",
    xaxis_title="Strikes",
    yaxis_title="Call / Put OI",
    height=700,
    bargap=0.3,
    barmode='overlay',
    xaxis=dict(
        tickangle=-45,
        categoryorder='category ascending'
    ),
    showlegend=False,
    hovermode='closest'
)

st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

legend_html = """
<div  style="display: flex; flex-wrap: wrap; justify-content: center; gap: 20px; align-items: center; font-size: 15px;">
    <div style="display: flex; align-items: center; gap: 5px;">
        <div style="width: 14px; height: 14px; background-color: #ef4444; border-radius: 2px;"></div> Put OI
    </div>
    <div style="display: flex; align-items: center; gap: 5px;">
        <div style="width: 14px; height: 14px; background-color: #ef4444; border-radius: 2px; background-image: repeating-linear-gradient(45deg, #fff 0, #fff 2px, #ef4444 2px, #ef4444 4px);"></div> Put Increase
    </div>
    <div style="display: flex; align-items: center; gap: 5px;">
        <div style="width: 14px; height: 14px; border: 2px solid #ef4444; border-radius: 2px;"></div> Put Decrease
    </div>
    <div style="display: flex; align-items: center; gap: 5px;">
        <div style="width: 14px; height: 14px; background-color: #22c55e; border-radius: 2px;"></div> Call OI
    </div>
    <div style="display: flex; align-items: center; gap: 5px;">
        <div style="width: 14px; height: 14px; background-color: #22c55e; border-radius: 2px; background-image: repeating-linear-gradient(45deg, #fff 0, #fff 2px, #22c55e 2px, #22c55e 4px);"></div> Call Increase
    </div>
    <div style="display: flex; align-items: center; gap: 5px;">
        <div style="width: 14px; height: 14px; border: 2px solid #22c55e; border-radius: 2px;"></div> Call Decrease
    </div>
</div>
"""

st.markdown(legend_html, unsafe_allow_html=True)


