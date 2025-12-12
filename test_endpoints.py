import asyncio
import json
import sys
import os

# Add current directory to path to use local version
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from twscrape import API

async def main():
    # Use the local accounts.db
    api = API("accounts.db")

    # Test 1: ListLatestTweetsTimeline
    print("Fetching list timeline for list_id=73804894...")
    try:
        gen = api.list_timeline_raw(73804894, limit=1)
        async for response in gen:
            with open("sample_list_timeline.json", "w") as f:
                json.dump(response, f, indent=2)
            print("✓ Saved sample_list_timeline.json")
            break
    except Exception as e:
        print(f"✗ Error fetching list timeline: {e}")

    # Test 2: CommunityTweetsTimeline
    print("\nFetching community timeline for community_id=1873453729988374929...")
    try:
        gen = api.community_timeline_raw("1873453729988374929", limit=1)
        async for response in gen:
            with open("sample_community_timeline.json", "w") as f:
                json.dump(response, f, indent=2)
            print("✓ Saved sample_community_timeline.json")
            break
    except Exception as e:
        print(f"✗ Error fetching community timeline: {e}")

    # Test 3: membersSliceTimeline_Query
    print("\nFetching community members for community_id=1958000335844438503...")
    try:
        gen = api.community_members_raw("1958000335844438503", limit=1)
        async for response in gen:
            with open("sample_community_members.json", "w") as f:
                json.dump(response, f, indent=2)
            print("✓ Saved sample_community_members.json")
            break
    except Exception as e:
        print(f"✗ Error fetching community members: {e}")

    # Test 4: AudioSpaceById
    print("\nFetching audio space for space_id=1djGXWOjWNVKZ...")
    try:
        response = await api.audio_space_raw("1djGXWOjWNVKZ")
        if response:
            with open("sample_audio_space.json", "w") as f:
                json.dump(response, f, indent=2)
            print("✓ Saved sample_audio_space.json")
        else:
            print("✗ No response from audio space endpoint")
    except Exception as e:
        print(f"✗ Error fetching audio space: {e}")

if __name__ == "__main__":
    asyncio.run(main())
