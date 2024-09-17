import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt

# Function to convert nanoseconds to milliseconds
def nanoseconds_to_milliseconds(nanos):
    return nanos / 1_000_000

# Function to process profile data
def process_profile_data(profile_data):
    # Initialize lists to store shard data
    shard_times = []
    total_time = 0
    query_times = {}
    children_times = {}
    
    # Iterate over shards
    for shard in profile_data.get('profile', {}).get('shards', []):
        index = shard.get('index', 'Unknown Index')
        shard_id = shard.get('shard_id', 'Unknown Shard')
        
        # Initialize times
        query_time = 0
        fetch_time = 0
        
        # Extract query times
        for search in shard.get('searches', []):
            for query in search.get('query', []):
                query_type = query.get('type', 'Unknown Query Type')
                description = query.get('description', 'No Description')
                time_in_nanos = query.get('time_in_nanos', 0)
                
                query_time += time_in_nanos
                
                field_name = extract_field_name(description)
                query_key = (query_type, field_name)
                
                if query_key not in query_times:
                    query_times[query_key] = 0
                query_times[query_key] += time_in_nanos
        
        # Extract fetch times
        fetch = shard.get('fetch', {})
        fetch_time = fetch.get('time_in_nanos', 0)
        
        # Calculate total time for this shard
        shard_total_time = query_time + fetch_time
        
        # Add to list and total time
        shard_times.append({
            'index': index,
            'shard_id': shard_id,
            'total_time_ms': nanoseconds_to_milliseconds(shard_total_time),
            'query_time_ms': nanoseconds_to_milliseconds(query_time),
            'fetch_time_ms': nanoseconds_to_milliseconds(fetch_time)
        })
        total_time += shard_total_time
        
        # Collect children times
        for search in shard.get('searches', []):
            for query in search.get('query', []):
                for child in query.get('children', []):
                    child_type = child.get('type', 'Unknown Type')
                    field_name = extract_field_name(child.get('description', 'No Description'))
                    time_in_nanos = child.get('time_in_nanos', 0)
                    
                    child_key = (child_type, field_name)
                    
                    if child_key not in children_times:
                        children_times[child_key] = 0
                    children_times[child_key] += time_in_nanos

    # Calculate percentages
    for shard in shard_times:
        shard['percentage'] = (shard['total_time_ms'] / nanoseconds_to_milliseconds(total_time)) * 100 if total_time > 0 else 0

    # Sort by percentage
    sorted_shard_times = sorted(shard_times, key=lambda x: x['percentage'], reverse=True)

    # Process query times
    sorted_query_times = sorted(query_times.items(), key=lambda x: x[1], reverse=True)
    
    # Process children times
    sorted_children_times = sorted(children_times.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_shard_times, total_time, sorted_query_times, sorted_children_times

# Function to extract field name from description
def extract_field_name(description):
    # Simple heuristic to extract field names from descriptions
    # This may need adjustment based on query formats
    if ':' in description:
        return description.split(':')[0]
    return 'Unknown Field'

# Function to plot bar chart
def plot_shard_times_bar_chart(shard_times):
    # Prepare data for plotting
    indices = [f"{shard['index']} - {shard['shard_id']}" for shard in shard_times]
    times = [shard['total_time_ms'] for shard in shard_times]
    
    fig, ax = plt.subplots()
    ax.barh(indices, times, color='skyblue')
    ax.set_xlabel('Time (ms)')
    ax.set_title('Shard Total Times')
    
    st.pyplot(fig)

# Streamlit UI
def main():
    st.title("Elasticsearch Profile Data Analyzer")
    
    # Text area for JSON input
    profile_output = st.text_area("Paste Elasticsearch Profile API Output Here", height=300)
    
    if profile_output:
        try:
            # Parse the input JSON
            profile_data = json.loads(profile_output)
            
            # Process the data
            shard_times, total_time, query_times, children_times = process_profile_data(profile_data)

            # Display the total query time as a big banner
            total_time_ms = nanoseconds_to_milliseconds(total_time)
            st.markdown(f"<h1 style='text-align: center; color: red;'>Total Query Time: {total_time_ms:.2f} ms</h1>", unsafe_allow_html=True)
            
            # Display Shard Times
            st.subheader("Shard Times")
            shard_df = pd.DataFrame(shard_times)
            st.dataframe(shard_df)

            # Plot Shard Times Bar Chart
            plot_shard_times_bar_chart(shard_times)
            
            # Display Query Times
            st.subheader("Query Times")
            query_times_df = pd.DataFrame(query_times, columns=['Query Type and Field', 'Time (ms)'])
            query_times_df['Time (ms)'] = query_times_df['Time (ms)'].apply(nanoseconds_to_milliseconds)
            st.dataframe(query_times_df)
            
            # Display Top 5 Children Types by Time
            st.subheader("Top 5 Children Types by Time")
            top_children_times = children_times[:5]  # Top 5 children types
            children_times_df = pd.DataFrame(top_children_times, columns=['Children Type and Field', 'Time (ms)'])
            children_times_df['Time (ms)'] = children_times_df['Time (ms)'].apply(nanoseconds_to_milliseconds)
            st.dataframe(children_times_df)

        except json.JSONDecodeError:
            st.error("Invalid JSON. Please check the format of your profile output.")
        except KeyError as e:
            st.error(f"Missing key in the data: {e}")

if __name__ == "__main__":
    main()
