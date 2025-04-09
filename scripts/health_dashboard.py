# Monthly Health Trends Dashboard (Apple Watch Data)
import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import time
import requests
from io import BytesIO



# Configures page title and layout and the page the title.
st.set_page_config(page_title="📊 Monthly Health Trends", layout="wide")

#Sets the main title at the top of dashboard
st.title("📊 Your Health Trends Over Time")

@st.cache_data
def load_data():

    base_url = "https://raw.githubusercontent.com/praneeth0810/AppleHealth-iWatch/main/transformed%20parq"

    files = {
        "heart": f"{base_url}/heart.parquet",
        "sleep": f"{base_url}/sleep.parquet",
        "resp": f"{base_url}/resp.parquet",
        "steps": f"{base_url}/step.parquet",
    }

    def read_parquet_from_github(url):
        response = requests.get(url)
        response.raise_for_status()
        return pd.read_parquet(BytesIO(response.content))

    heart = read_parquet_from_github(files["heart"])
    sleep = read_parquet_from_github(files["sleep"])
    resp = read_parquet_from_github(files["resp"])
    steps = read_parquet_from_github(files["steps"])

    for df in [heart, sleep, resp, steps]:
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["year"] = df["created_at"].dt.year
        df["month"] = df["created_at"].dt.month
        df["weekday"] = df["created_at"].dt.day_name()

    return heart, sleep, resp, steps

heart_df, sleep_df, resp_df, steps_df = load_data()

# Month label dictionary
month_labels = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}
label_to_month = {v: k for k, v in month_labels.items()}
weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Streamlit UI
with st.sidebar:
    st.header("📅 Filter")
    year = st.selectbox("Year", sorted(heart_df["year"].unique(), reverse=True))
    month_name = st.selectbox("Month", [month_labels[m] for m in sorted(heart_df["month"].unique())])
    month = label_to_month[month_name]

tabs = st.tabs(["🫀 Heart", "😴 Sleep", "😮‍💨 Respiration", "🚶 Steps"])

# ----------------- HEART TAB -----------------
with tabs[0]:
    st.subheader("🫀 How was my heart rate this month?")
    st.markdown("""This section shows your average heart rate day by day, the distribution across different heart rate zones (resting, normal, high), and how your HR varies by weekday. Look for patterns like elevated HR on certain weekdays or significant outliers.""")

    df = heart_df[(heart_df["year"] == year) & (heart_df["month"] == month)]

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Average HR", f"{df['avg_heart_rate'].mean():.1f} bpm")
    with col2:
        st.metric("Min/Max HR", f"{df['avg_heart_rate'].min():.0f} / {df['avg_heart_rate'].max():.0f} bpm")

    st.altair_chart(
        alt.Chart(df).mark_line(point=True).encode(
            x=alt.X("created_at:T", title="Date"),
            y=alt.Y("avg_heart_rate:Q", title="Average Heart Rate (bpm)"),
            tooltip=["created_at:T", "avg_heart_rate"]
        ).properties(title="📈 Daily Heart Rate Trend", height=300),
        use_container_width=True
    )

    def zone(hr):
        return "Resting" if hr < 60 else "Normal" if hr <= 90 else "High"
    df["zone"] = df["avg_heart_rate"].apply(zone)
    zone_counts = df["zone"].value_counts().reset_index()
    zone_counts.columns = ["HR Zone", "Days"]
    fig_zone = px.bar(zone_counts, x="HR Zone", y="Days", color="HR Zone",
                      title="🧭 Heart Rate Zone Breakdown")
    st.plotly_chart(fig_zone, use_container_width=True)

    st.subheader("📦 Heart Rate by Day of Week")
    boxplot = alt.Chart(df).mark_boxplot(size=100).encode(
        x=alt.X("weekday:N", sort=weekday_order, title="Day of Week"),
        y=alt.Y("avg_heart_rate:Q", title="Heart Rate (bpm)"),
        tooltip=["weekday", "avg_heart_rate"]
    ).properties(title="📦 Distribution of Heart Rate by Weekday", height=400)
    st.altair_chart(boxplot, use_container_width=True)

# ----------------- SLEEP TAB -----------------
with tabs[1]:
    st.subheader("😴 Did I sleep enough and consistently this month?")
    st.markdown("""This section shows how much you slept each day and highlights your best night of sleep. It also shows your weekly sleep patterns to spot trends across the month.""")
    df = sleep_df[(sleep_df["year"] == year) & (sleep_df["month"] == month)]

    total_sleep_hr = df["total_sleep_minutes"].mean() / 60
    quality = "Good" if total_sleep_hr >= 7 else "Fair" if total_sleep_hr >= 6 else "Poor"
    st.metric("Avg Sleep (hrs)", f"{total_sleep_hr:.1f}", help="7–9 hrs is recommended")
    st.success(f"Sleep Quality: {quality}")



    df["week"] = df["created_at"].dt.isocalendar().week
    week_avg = df.groupby("week")["total_sleep_minutes"].mean().reset_index()
    st.subheader("🥱 Sleep Duration per Week")
    st.bar_chart(week_avg.set_index("week"), height=250)

    st.subheader("🌙 Daily Sleep Duration")
    st.altair_chart(
        alt.Chart(df).mark_bar().encode(
            x=alt.X("created_at:T", title="Date"),
            y=alt.Y("total_sleep_minutes:Q", title="Sleep Duration (minutes)"),
            tooltip=["created_at:T", "total_sleep_minutes"]
        ).properties(title="🌙 Sleep per Night", height=300),
        use_container_width=True
    )

    if not df.empty:
        best_sleep = df.loc[df["total_sleep_minutes"].idxmax()]
        st.info(f"Best Sleep: {best_sleep['total_sleep_minutes']:.0f} minutes on {best_sleep['created_at'].date()}")


# ----------------- RESPIRATION TAB -----------------
with tabs[2]:
    st.subheader("😮‍💨 Was my breathing stable this month?")
    st.markdown("""This chart includes your daily respiratory rate and a smoothed 7-day moving average. Look out for days with very low or high values that may indicate irregular breathing.""")
    df = resp_df[(resp_df["year"] == year) & (resp_df["month"] == month)].copy()
    df = df.sort_values("created_at")
    df["rolling"] = df["avg_resp_rate"].rolling(7).mean()

    st.altair_chart(
        alt.Chart(df).transform_fold(["avg_resp_rate", "rolling"], as_=["Type", "Value"]).mark_line().encode(
            x="created_at:T",
            y=alt.Y("Value:Q", title="Respiratory Rate (bpm)"),
            color=alt.Color("Type:N", title="Legend")
        ).properties(title="📈 Respiration Rate Trend", height=300),
        use_container_width=True
    )

    abnormal = df[(df["avg_resp_rate"] < 10) | (df["avg_resp_rate"] > 25)]
    if not abnormal.empty:
        st.warning("Abnormal Respiration Days:")
        st.dataframe(abnormal[["created_at", "avg_resp_rate"]])
    else:
        st.success("✅ No abnormal respiration days this month.")

# ----------------- STEPS TAB -----------------
with tabs[3]:
    st.subheader("🚶 Was I active enough this month?")
    st.markdown("""You can see your daily step counts, how often you reached your step goal, and whether there were days with low movement (sedentary). Patterns by weekday can highlight habits. Days are now sorted correctly from Monday to Sunday.""")
    df = steps_df[(steps_df["year"] == year) & (steps_df["month"] == month)].copy()
    df["weekday"] = pd.Categorical(df["created_at"].dt.day_name(), categories=weekday_order, ordered=True)


    st.altair_chart(
        alt.Chart(df).mark_line(point=True).encode(
            x=alt.X("created_at:T", title="Date"),
            y=alt.Y("total_steps:Q", title="Steps")
        ).properties(height=300, title="📈 Daily Steps vs Goal"),
        use_container_width=True
    )

    goal = 7500
    active_days = (df["total_steps"] >= goal).sum()
    inactive_days = (df["total_steps"] < goal).sum()
    fig_steps = px.pie(values=[active_days, inactive_days],
                       names=["Reached Goal", "Below Goal"],
                       title=f"📊 Days Meeting Step Goal ({goal}+)", hole=0.4)
    st.plotly_chart(fig_steps, use_container_width=True)

    df["hit_goal"] = df["total_steps"] >= goal
    goal_by_day = df.groupby("weekday", observed=True)["hit_goal"].sum()
    st.subheader("✅ Goal Hits by Weekday")
    st.bar_chart(goal_by_day, height=250)

    weekday_avg = df.groupby("weekday", observed=True)["total_steps"].mean()
    st.subheader("📊 Average Steps by Weekday")
    st.bar_chart(weekday_avg, height=250)

    sedentary = df[df["total_steps"] < 2000]
    if not sedentary.empty:
        st.error("🚨 Sedentary Days (< 2,000 steps)")
        st.dataframe(sedentary[["created_at", "total_steps"]])
    else:
        st.success("✅ No sedentary days this month.")

st.markdown("""
    <div style='position: fixed; bottom: 10px; right: 15px; color: gray; font-size: 0.85em;'>
        Created by Praneeth Chavva
    </div>
""", unsafe_allow_html=True)
