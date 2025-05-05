def read_video_links(input_file_path):
    """Read YouTube video links from the input file."""
    video_links = []
    with open(input_file_path, 'r') as file:
        for line in file:
            video_links.append(line.strip())
    return video_links

def generate_html(video_links, output_file_path, videos_per_page=10):
    """Generate HTML page with video links stored in a JavaScript array and pagination."""
    total_videos = len(video_links)
    total_pages = (total_videos // videos_per_page) + (1 if total_videos % videos_per_page > 0 else 0)

    # Start HTML content
    html_content = '''
    <html>
    <head>
        <title>YouTube Videos</title>
        <style>
            .video-container { display: flex; flex-wrap: wrap; gap: 20px; }
            .video { width: 200px; }
            iframe { width: 100%; height: 150px; border: none; }
            .pagination { margin-top: 20px; text-align: center; }
            .pagination a { margin: 0 10px; text-decoration: none; font-size: 18px; cursor: pointer; }
        </style>
    </head>
    <body>
        <h1>YouTube Videos</h1>
        <div class="video-container" id="video-container"></div>

        <div class="pagination">
    '''

    # Add pagination controls
    for page in range(1, total_pages + 1):
        html_content += f'<a onclick="loadPage({page})">{page}</a>'

    # Add JavaScript for pagination and loading videos
    html_content += '''
        </div>

        <script>
            const videoLinks = ''' + str(video_links) + ''';

            function loadPage(pageNumber) {
                const videosPerPage = ''' + str(videos_per_page) + ''';
                const startIdx = (pageNumber - 1) * videosPerPage;
                const endIdx = Math.min(startIdx + videosPerPage, videoLinks.length);
                const container = document.getElementById("video-container");

                container.innerHTML = ""; // Clear current videos

                for (let i = startIdx; i < endIdx; i++) {
                    const videoUrl = videoLinks[i];
                    const videoId = new URL(videoUrl).searchParams.get("v");

                    const videoElement = document.createElement("div");
                    videoElement.classList.add("video");
                    videoElement.innerHTML = `
                        <iframe src="https://www.youtube.com/embed/${videoId}" title="YouTube video"></iframe>
                    `;
                    container.appendChild(videoElement);
                }
            }

            // Load the first page initially
            loadPage(1);
        </script>
    </body>
    </html>
    '''

    # Save the HTML content to the output file
    with open(output_file_path, 'w') as output_file:
        output_file.write(html_content)

    print(f"HTML page has been generated and saved to {output_file_path}.")

# Example usage
input_file_path = "C:/Users/USER/Downloads/unique_youtube_links_valid.txt"  # Input file containing YouTube links
output_file_path = "C:/Users/USER/Downloads/youtube_video_page.html"  # Output HTML file to save

# Generate the HTML page with videos and pagination
video_links = read_video_links(input_file_path)
generate_html(video_links, output_file_path)


exit(0)

def extract_video_id(youtube_url):
    """Extract the video ID from a YouTube URL."""
    if 'v=' in youtube_url:
        return youtube_url.split('v=')[-1]
    else:
        return None


def remove_duplicate_videos(input_file_path, output_file_path):
    """Remove duplicate YouTube video links based on video IDs and generate new links."""
    unique_video_ids = set()  # To store unique video IDs
    new_youtube_links = []  # To store new YouTube links

    # Read all YouTube links from the input file
    with open(input_file_path, 'r') as file:
        for line in file:
            youtube_url = line.strip()  # Remove any leading/trailing whitespace
            if youtube_url:
                video_id = extract_video_id(youtube_url)
                if video_id and video_id not in unique_video_ids:
                    unique_video_ids.add(video_id)
                    new_youtube_links.append(f"http://www.youtube.com/watch?v={video_id}")

    # Write the new YouTube links to the output file
    with open(output_file_path, 'w') as output_file:
        for link in new_youtube_links:
            output_file.write(f"{link}\n")

    print(f"Done! New YouTube links have been written to {output_file_path}.")


# Example usage
input_file_path = "C:/Users/USER/Downloads/youtube_links_valid.txt"  # Input file containing YouTube links
output_file_path = "C:/Users/USER/Downloads/unique_youtube_links_valid.txt"  # Output file to save new links

# Remove duplicate video links and generate new links
remove_duplicate_videos(input_file_path, output_file_path)

exit(0)
import requests


def extract_video_id(youtube_url):
    """Extract the video ID from a YouTube URL."""
    return youtube_url.split('v=')[-1]


def check_youtube_video_status(video_id, api_key):
    """Check if a YouTube video is valid using YouTube Data API."""
    api_url = f"https://www.googleapis.com/youtube/v3/videos?id={video_id}&key={api_key}&part=status"

    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        if len(data['items']) == 0:
            return False  # The video is invalid or removed
        else:
            # Check if the video is public and processed
            video_status = data['items'][0]['status']
            upload_status = video_status.get('uploadStatus', 'unknown')
            privacy_status = video_status.get('privacyStatus', 'unknown')
            return upload_status == 'processed' and privacy_status == 'public'
    else:
        return False  # Unable to retrieve video data (error case)


def check_videos_in_file(input_file_path, output_file_path, api_key):
    """Check all YouTube video links in a text file and write valid links to a new file."""
    valid_links = []

    with open(input_file_path, 'r') as file:
        for line in file:
            video_url = line.strip()  # Remove any leading/trailing whitespace
            if video_url:
                video_id = extract_video_id(video_url)
                is_valid = check_youtube_video_status(video_id, api_key)
                if is_valid:
                    valid_links.append(video_url)

    # Write valid links to a new text file
    with open(output_file_path, 'w') as output_file:
        for link in valid_links:
            output_file.write(f"{link}\n")

    print(f"Done! Valid links have been written to {output_file_path}.")


# Example usage
api_key = "AIzaSyB5bSQDCHTnj3HZhTheYFKaI3VtIwSgz08"  # Replace with your actual YouTube API key
input_file_path = "C:/Users/USER/Downloads/youtube_links (1).txt"  # Input file containing YouTube links
output_file_path = "C:/Users/USER/Downloads/youtube_links_valid.txt"  # Output file to save valid links

# Check videos and save valid links
check_videos_in_file(input_file_path, output_file_path, api_key)
