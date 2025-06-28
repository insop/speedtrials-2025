import streamlit as st, sqlite3, pandas as pd, pydeck as pdk, altair as alt

DB   = "georgia_water.db"
PAGE = st.sidebar.selectbox("Page", ["Overview","Search","Map"])

@st.cache_data
def query(sql, *params):
    con = sqlite3.connect(DB)
    return pd.read_sql_query(sql, con, params=params)

########################################################################
# OVERVIEW
########################################################################
if PAGE == "Overview":
    st.title("Georgia Drinking Water – Overview")
    st.markdown("Live data from SDWIS, cleaned & queryable.")
    total = query("select count(*) c from water_systems")["c"][0]
    viol  = query("select count(*) v from violations")["v"][0]
    col1,col2 = st.columns(2)
    col1.metric(":droplet: Water Systems", f"{total:,}")
    col2.metric(":warning: Total Violations", f"{viol:,}")

    # bar chart – top contaminants
    top_contam = query("""
        select c.contaminant_code, c.contaminant_name, count(*) n
        from monitoring_results r
        join contaminants c using(contaminant_code)
        group by 1,2 order by n desc limit 15
    """)
    ch = alt.Chart(top_contam).mark_bar().encode(
        x=alt.X("n:Q", title="Samples"),
        y=alt.Y("contaminant_name:N", sort='-x', title="")
    )
    st.altair_chart(ch, use_container_width=True)

########################################################################
# SEARCH
########################################################################
elif PAGE == "Search":
    st.title("🔎 Water System Search")
    txt = st.text_input("System name or PWS ID")
    if txt:
        df = query("""
            SELECT pws_id, pws_name, county_served, population_served_count
            FROM water_systems
            WHERE water_systems MATCH ? || '*'
            ORDER BY rank LIMIT 25
        """, txt)
        st.dataframe(df.set_index("pws_id"))
        if len(df)==1:
            pws = df.iloc[0]["pws_id"]
            st.subheader(f"Violations for {pws}")
            viol = query("select * from violations where pws_id = ?", pws)
            st.dataframe(viol)

########################################################################
# MAP
########################################################################
elif PAGE == "Map":
    st.title("🗺️  State-wide Map")
    rows = query("""
        select ws.pws_id, ws.pws_name, coalesce(ct.lat,34) lat, coalesce(ct.lon,-83) lon,
               case when exists(select 1 from violations v where v.pws_id=ws.pws_id)
                    then 'Violation' else 'Compliant' end status
        from water_systems ws
        left join counties ct on ct.county = ws.county_served
        limit 5000
    """)
    layer = pdk.Layer(
        "ScatterplotLayer",
        rows,
        get_position='[lon, lat]',
        get_fill_color="['Violation'==status ? 255 : 0, 30, 100]",
        get_radius=2500,
        pickable=True,
    )
    st.pydeck_chart(pdk.Deck(map_style="mapbox://styles/mapbox/light-v10",
                             initial_view_state=pdk.ViewState(latitude=32.9, longitude=-83.5, zoom=6),
                             layers=[layer], tooltip={"text": "{pws_name}\n{status}"}))