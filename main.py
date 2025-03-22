import httpx
import json
import csv
from typing import Optional

class DiscordThreadScraper:
    def __init__(self, token: str):
        self.base_url = "https://discord.com/api/v9"
        self.headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        self.tag_mapping = {}  # To store tag ID-to-name mappings

    async def fetch_channel_metadata(self, channel_id: str):
        """Fetch channel metadata to retrieve available tags."""
        async with httpx.AsyncClient() as client:
            url = f"{self.base_url}/channels/{channel_id}"
            response = await client.get(url, headers=self.headers)

            if response.status_code != 200:
                print(f"Failed to fetch metadata for channel {channel_id} (Status {response.status_code})")
                return

            data = response.json()
            # Build a mapping of tag IDs to friendly tag names
            self.tag_mapping = {
                tag["id"]: tag["name"]
                for tag in data.get("available_tags", [])
            }
            print(f"Fetched {len(self.tag_mapping)} tags from channel metadata.")

    async def get_threads(self, channel_id: str, archived: bool, after: Optional[str] = None):
        """Fetch threads (active or archived) for the channel."""
        current_offset = 0
        fetch_delay = 0.1
        all_threads = []

        async with httpx.AsyncClient() as client:
            while True:
                url = f"{self.base_url}/channels/{channel_id}/threads/search"
                params = {
                    "sort_by": "last_message_time",
                    "sort_order": "desc",
                    "archived": str(archived).lower(),
                    "offset": current_offset
                }

                print(f"Requesting {'archived' if archived else 'active'} threads with offset: {current_offset}")



        
                response = await client.get(url, headers=self.headers, params=params)

                # Handle rate limits (429 Too Many Requests)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", 1)
                    print(f"Rate limited! Retrying after {retry_after} seconds...")
                    new_delay = float(retry_after)
                    await asyncio.sleep(new_delay)
                    fetch_delay = fetch_delay + new_delay  # delay increases, never decreases
                    continue  # Retry the same request
                    
                if response.status_code != 200:
                    print(f"Failed to access channel {channel_id} (Status {response.status_code})")
                    break

                data = response.json()
                threads = data.get("threads", [])
                if not threads:
                    print(f"No more {'archived' if archived else 'active'} threads found.")
                    break

                all_threads.extend(threads)
                print(f"Fetched {len(threads)} {'archived' if archived else 'active'} threads, total so far: {len(all_threads)}")

                current_offset += len(threads)
                if not data.get("has_more", False):
                    print(f"No more {'archived' if archived else 'active'} threads to fetch (has_more=False).")
                    break
                    
                # Add a delay to prevent hitting rate limits
                await asyncio.sleep(fetch_delay) 
        
        return all_threads

    async def scrape_all_threads(self, channel_id: str):
        """Scrape threads (both active and archived) and save to JSON/CSV."""
        # Fetch channel metadata to build the tag mapping
        await self.fetch_channel_metadata(channel_id)

        # Fetch active threads
        print("Starting to scrape active threads...")
        active_threads = await self.get_threads(channel_id, archived=False)

        # Fetch archived threads
        print("Starting to scrape archived threads...")
        archived_threads = await self.get_threads(channel_id, archived=True)

        all_threads = active_threads + archived_threads

        # Save to files
        self.save_to_json(all_threads)
        self.save_to_csv(all_threads)

    @staticmethod
    def save_to_json(threads):
        """Save threads to a JSON file."""
        with open("threads.json", "w") as outfile:
            json.dump(threads, outfile, indent=4)
        print("Threads saved to threads.json")

    def save_to_csv(self, threads):
        """Save threads to a CSV file."""
        with open("threads.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["Name", "Created", "Messages", "Tags"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for thread in threads:
                # Map applied tags from IDs to friendly names
                applied_tags = thread.get("applied_tags", [])
                friendly_tags = [self.tag_mapping.get(tag_id, f"Unknown ({tag_id})") for tag_id in applied_tags]

                # Format the created_at date to YYYY-MM-DD
                full_timestamp = thread.get("thread_metadata", {}).get("create_timestamp", "Unknown")
                formatted_date = full_timestamp.split("T")[0] if "T" in full_timestamp else "Unknown"

                writer.writerow({
                    "Name": thread.get("name", "Unknown"),
                    "Created": formatted_date,
                    "Messages": thread.get("message_count", 0),
                    "Tags": ", ".join(friendly_tags) if friendly_tags else "None"
                })

        print("Threads saved to threads.csv")


# Usage example
if __name__ == "__main__":
    import asyncio
    TOKEN = "Your Token Here"
    CHANNEL_ID = "1263394639524921414"  # flat2vr uuvr channel
    #CHANNEL_ID = "1062167556129030164"  # flat2vr ue-games channel

    async def scrape_threads():
        scraper = DiscordThreadScraper(TOKEN)
        await scraper.scrape_all_threads(CHANNEL_ID)

    asyncio.run(scrape_threads())

