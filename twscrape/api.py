from contextlib import aclosing
from typing import Literal

from httpx import Response

from .accounts_pool import AccountsPool
from .logger import set_log_level
from .models import Tweet, User, parse_trends, parse_tweet, parse_tweets, parse_user, parse_users
from .queue_client import QueueClient
from .utils import encode_params, find_obj, get_by_path

# OP_{NAME} – {NAME} should be same as second part of GQL ID (required to auto-update script)
OP_SearchTimeline = "bshMIjqDk8LTXTq4w91WKw/SearchTimeline"
OP_UserByRestId = "WJ7rCtezBVT6nk6VM5R8Bw/UserByRestId"
OP_UserByScreenName = "-oaLodhGbbnzJBACb1kk2Q/UserByScreenName"
OP_TweetDetail = "6QzqakNMdh_YzBAR9SYPkQ/TweetDetail"
OP_Followers = "SCu9fVIlCUm-BM8-tL5pkQ/Followers"
OP_Following = "S5xUN9s2v4xk50KWGGvyvQ/Following"
OP_Retweeters = "IQ43ps3iEcdrGV_OL1QaRw/Retweeters"
OP_UserTweets = "lZRf8IC-GTuGxDwcsHW8aw/UserTweets"
OP_UserTweetsAndReplies = "gXCeOBFsTOuimuCl1qXimg/UserTweetsAndReplies"
OP_ListLatestTweetsTimeline = "BkauSnPUDQTeeJsxq17opA/ListLatestTweetsTimeline"
OP_CommunityTweetsTimeline = "mvvfN7tozrFnot9Rfbp_Mw/CommunityTweetsTimeline"
OP_CommunityMediaTimeline = "DJ2AxDtvus2BfW5AvOhtaw/CommunityMediaTimeline"
OP_membersSliceTimeline_Query = "WSbJGJjZaVasSj9bnqSZSA/membersSliceTimeline_Query"
OP_AudioSpaceById = "rC2zlE1t7SHbVG8obPZliQ/AudioSpaceById"
OP_AboutAccountQuery = "zs_jFPFT78rBpXv9Z3U2YQ/AboutAccountQuery"
OP_BlueVerifiedFollowers = "ZpmVpf_fBIUgdPErpq2wWg/BlueVerifiedFollowers"
OP_UserCreatorSubscriptions = "7qcGrVKpcooih_VvJLA1ng/UserCreatorSubscriptions"
OP_UserMedia = "vFPc2LVIu7so2uA_gHQAdg/UserMedia"
OP_Bookmarks = "-LGfdImKeQz0xS_jjUwzlA/Bookmarks"
OP_GenericTimelineById = "CT0YFEFf5GOYa5DJcxM91w/GenericTimelineById"
OP_ProfileSpotlightsQuery = "mzoqrVGwk-YTSGME1dRfXQ/ProfileSpotlightsQuery"
OP_CreateTweet = "Uf3io9zVp1DsYxrmL5FJ7g/CreateTweet"

GQL_URL = "https://x.com/i/api/graphql"
REST_URL = "https://x.com/i/api/1.1"
GQL_FEATURES = {  # search values here (view source) https://x.com/
    "articles_preview_enabled": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "communities_web_enable_tweet_community_results_fetch": True,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "premium_content_api_read_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_grok_analysis_button_from_backend": True,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": True,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_grok_image_annotation_enabled": True,
    "responsive_web_grok_imagine_annotation_enabled": True,
    "responsive_web_grok_share_attachment_enabled": True,
    "responsive_web_grok_show_grok_translated_post": True,
    "responsive_web_jetfuel_frame": True,
    "responsive_web_profile_redirect_enabled": False,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "rweb_tipjar_consumption_enabled": True,
    "rweb_video_screen_enabled": False,
    "standardized_nudges_misinfo": True,
    "tweet_awards_web_tipping_enabled": False,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "verified_phone_label_enabled": False,
    "view_counts_everywhere_api_enabled": True,
}

KV = dict | None
TrendId = Literal["trending", "news", "sport", "entertainment"] | str


class API:
    # Note: kv is variables, ft is features from original GQL request
    pool: AccountsPool

    def __init__(
        self,
        pool: AccountsPool | str | None = None,
        debug=False,
        proxy: str | None = None,
        raise_when_no_account=False,
    ):
        if isinstance(pool, AccountsPool):
            self.pool = pool
        elif isinstance(pool, str):
            self.pool = AccountsPool(db_file=pool, raise_when_no_account=raise_when_no_account)
        else:
            self.pool = AccountsPool(raise_when_no_account=raise_when_no_account)

        self.proxy = proxy
        self.debug = debug
        if self.debug:
            set_log_level("DEBUG")

    # general helpers

    def _is_end(self, rep: Response, q: str, res: list, cur: str | None, cnt: int, lim: int):
        new_count = len(res)
        new_total = cnt + new_count

        is_res = new_count > 0
        is_cur = cur is not None
        is_lim = lim > 0 and new_total >= lim

        return rep if is_res else None, new_total, is_cur and not is_lim

    def _get_cursor(self, obj: dict, cursor_type="Bottom") -> str | None:
        if cur := find_obj(obj, lambda x: x.get("cursorType") == cursor_type):
            return cur.get("value")
        return None

    # gql helpers

    async def _gql_items(
        self, op: str, kv: dict, ft: dict | None = None, limit=-1, cursor_type="Bottom"
    ):
        queue, cur, cnt, active = op.split("/")[-1], None, 0, True
        kv, ft = {**kv}, {**GQL_FEATURES, **(ft or {})}

        async with QueueClient(self.pool, queue, self.debug, proxy=self.proxy) as client:
            while active:
                params = {"variables": kv, "features": ft}
                if cur is not None:
                    params["variables"]["cursor"] = cur
                if queue in ("SearchTimeline", "ListLatestTweetsTimeline", "CommunityTweetsTimeline"):
                    params["fieldToggles"] = {"withArticleRichContentState": False}
                if queue in ("UserMedia", "UserTweets", "UserTweetsAndReplies"):
                    params["fieldToggles"] = {"withArticlePlainText": False}
                if queue in ("TweetDetail",):
                    params["fieldToggles"] = {"withArticleRichContentState": True, "withArticlePlainText": False, "withGrokAnalyze": False, "withDisallowedReplyControls": False}

                rep = await client.get(f"{GQL_URL}/{op}", params=encode_params(params))
                if rep is None:
                    return

                obj = rep.json()
                els = get_by_path(obj, "entries") or []
                els = [
                    x
                    for x in els
                    if not (
                        x["entryId"].startswith("cursor-")
                        or x["entryId"].startswith("messageprompt-")
                    )
                ]
                cur = self._get_cursor(obj, cursor_type)

                rep, cnt, active = self._is_end(rep, queue, els, cur, cnt, limit)
                if rep is None:
                    return

                yield rep

    async def _gql_items_with_cursor(
        self, op: str, kv: dict, ft: dict | None = None, limit=-1, cursor_type="Bottom"
    ):
        """
        Same as _gql_items but yields (response, cursor) tuples.
        This allows callers to track pagination cursors for resumption.
        """
        queue, cur, cnt, active = op.split("/")[-1], None, 0, True
        kv, ft = {**kv}, {**GQL_FEATURES, **(ft or {})}

        async with QueueClient(self.pool, queue, self.debug, proxy=self.proxy) as client:
            while active:
                params = {"variables": kv, "features": ft}
                if cur is not None:
                    params["variables"]["cursor"] = cur
                if queue in ("SearchTimeline", "ListLatestTweetsTimeline", "CommunityTweetsTimeline"):
                    params["fieldToggles"] = {"withArticleRichContentState": False}
                if queue in ("UserMedia", "UserTweets", "UserTweetsAndReplies"):
                    params["fieldToggles"] = {"withArticlePlainText": False}
                if queue in ("TweetDetail",):
                    params["fieldToggles"] = {"withArticleRichContentState": True, "withArticlePlainText": False, "withGrokAnalyze": False, "withDisallowedReplyControls": False}

                rep = await client.get(f"{GQL_URL}/{op}", params=encode_params(params))
                if rep is None:
                    return

                obj = rep.json()
                els = get_by_path(obj, "entries") or []
                els = [
                    x
                    for x in els
                    if not (
                        x["entryId"].startswith("cursor-")
                        or x["entryId"].startswith("messageprompt-")
                    )
                ]
                next_cursor = self._get_cursor(obj, cursor_type)

                rep, cnt, active = self._is_end(rep, queue, els, next_cursor, cnt, limit)
                if rep is None:
                    return

                # Yield both response AND cursor for resumption
                yield rep, next_cursor

                # CRITICAL: Update cur for next iteration to advance pagination
                cur = next_cursor

    async def _gql_item(self, op: str, kv: dict, ft: dict | None = None, field_toggles: dict | None = None):
        ft = ft or {}
        queue = op.split("/")[-1]
        async with QueueClient(self.pool, queue, self.debug, proxy=self.proxy) as client:
            params = {"variables": {**kv}, "features": {**GQL_FEATURES, **ft}}
            if field_toggles:
                params["fieldToggles"] = field_toggles
            return await client.get(f"{GQL_URL}/{op}", params=encode_params(params))

    async def _gql_mutation(self, op: str, kv: dict, ft: dict | None = None):
        """Execute a GraphQL mutation (POST request with JSON body)."""
        ft = ft or {}
        queue = op.split("/")[-1]
        async with QueueClient(self.pool, queue, self.debug, proxy=self.proxy) as client:
            payload = {
                "variables": {**kv},
                "features": {**GQL_FEATURES, **ft},
                "queryId": op.split("/")[0],
            }
            return await client.post(f"{GQL_URL}/{op}", payload)

    async def _rest_get(self, endpoint: str, params: dict, queue: str | None = None):
        """Execute a REST API GET request."""
        queue = queue or endpoint.split("/")[-1].replace(".json", "")
        async with QueueClient(self.pool, queue, self.debug, proxy=self.proxy) as client:
            return await client.get(f"{REST_URL}/{endpoint}", params=params)

    # search

    async def search_raw(self, q: str, limit=-1, kv: KV = None):
        op = OP_SearchTimeline
        kv = {
            "rawQuery": q,
            "count": 20,
            "product": "Latest",
            "querySource": "typed_query",
            "withGrokTranslatedBio": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def search(self, q: str, limit=-1, kv: KV = None):
        async with aclosing(self.search_raw(q, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep.json(), limit):
                    yield x

    async def search_with_cursor(self, q: str, limit=-1, kv: KV = None):
        """
        Same as search but yields (tweet, cursor) tuples for resumable pagination.

        The cursor can be saved and passed back in kv={'cursor': saved_cursor}
        to resume pagination from where you left off.

        Example:
            last_cursor = None
            async for tweet, cursor in api.search_with_cursor("python", limit=1000):
                print(tweet.rawContent)
                last_cursor = cursor
            # Save last_cursor to database for resumption
        """
        op = OP_SearchTimeline
        kv = {
            "rawQuery": q,
            "count": 20,
            "product": "Latest",
            "querySource": "typed_query",
            "withGrokTranslatedBio": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items_with_cursor(op, kv, limit=limit)) as gen:
            async for rep, cursor in gen:
                for tweet in parse_tweets(rep.json(), limit):
                    yield tweet, cursor

    async def search_user(self, q: str, limit=-1, kv: KV = None):
        kv = {"product": "People", **(kv or {})}
        async with aclosing(self.search_raw(q, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_users(rep.json(), limit):
                    yield x

    # user_by_id

    async def user_by_id_raw(self, uid: int, kv: KV = None):
        op = OP_UserByRestId
        kv = {"userId": str(uid), "withSafetyModeUserFields": True, **(kv or {})}
        ft = {
            "hidden_profile_likes_enabled": True,
            "highlights_tweets_tab_ui_enabled": True,
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "hidden_profile_subscriptions_enabled": True,
            "responsive_web_twitter_article_notes_tab_enabled": False,
            "subscriptions_feature_can_gift_premium": False,
            "profile_label_improvements_pcf_label_in_post_enabled": False,
        }
        return await self._gql_item(op, kv, ft)

    async def user_by_id(self, uid: int, kv: KV = None) -> User | None:
        rep = await self.user_by_id_raw(uid, kv=kv)
        return parse_user(rep) if rep else None

    # user_by_login

    async def user_by_login_raw(self, login: str, kv: KV = None):
        op = OP_UserByScreenName
        kv = {"screen_name": login, "withGrokTranslatedBio": True, **(kv or {})}
        ft = {
            "hidden_profile_subscriptions_enabled": True,
            "profile_label_improvements_pcf_label_in_post_enabled": True,
            "responsive_web_profile_redirect_enabled": False,
            "rweb_tipjar_consumption_enabled": True,
            "verified_phone_label_enabled": False,
            "subscriptions_verification_info_is_identity_verified_enabled": True,
            "subscriptions_verification_info_verified_since_enabled": True,
            "highlights_tweets_tab_ui_enabled": True,
            "responsive_web_twitter_article_notes_tab_enabled": True,
            "subscriptions_feature_can_gift_premium": True,
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
            "responsive_web_graphql_timeline_navigation_enabled": True,
        }
        field_toggles = {"withPayments": False, "withAuxiliaryUserLabels": True}
        return await self._gql_item(op, kv, ft, field_toggles)

    async def user_by_login(self, login: str, kv: KV = None) -> User | None:
        rep = await self.user_by_login_raw(login, kv=kv)
        return parse_user(rep) if rep else None

    # tweet_details

    async def tweet_details_raw(self, twid: int, kv: KV = None):
        op = OP_TweetDetail
        kv = {
            "focalTweetId": str(twid),
            "with_rux_injections": False,
            "includePromotedContent": True,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
            **(kv or {}),
        }
        return await self._gql_item(op, kv)

    async def tweet_details(self, twid: int, kv: KV = None) -> Tweet | None:
        rep = await self.tweet_details_raw(twid, kv=kv)
        return parse_tweet(rep, twid) if rep else None

    # tweet_replies
    # note: uses same op as tweet_details, see: https://github.com/vladkens/twscrape/issues/104

    async def tweet_replies_raw(self, twid: int, limit=-1, kv: KV = None):
        op = OP_TweetDetail
        kv = {
            "focalTweetId": str(twid),
            "referrer": "tweet",
            "with_rux_injections": False,
            "includePromotedContent": True,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
            **(kv or {}),
        }
        async with aclosing(
            self._gql_items(op, kv, limit=limit, cursor_type="ShowMoreThreads")
        ) as gen:
            async for x in gen:
                yield x

    async def tweet_replies(self, twid: int, limit=-1, kv: KV = None):
        async with aclosing(self.tweet_replies_raw(twid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep.json(), limit):
                    if x.inReplyToTweetId == twid:
                        yield x

    # followers

    async def followers_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_Followers
        kv = {
            "userId": str(uid),
            "count": 20,
            "includePromotedContent": False,
            "withGrokTranslatedBio": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def followers(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(self.followers_raw(uid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_users(rep.json(), limit):
                    yield x

    async def followers_with_cursor(self, uid: int, limit=-1, kv: KV = None):
        """
        Same as followers but yields (user, cursor) tuples.

        The cursor can be saved and passed back in kv={'cursor': saved_cursor}
        to resume pagination from where you left off.

        Example:
            last_cursor = None
            async for user, cursor in api.followers_with_cursor(user_id, limit=1000):
                print(user.username)
                last_cursor = cursor
            # Save last_cursor to database for resumption
        """
        op = OP_Followers
        kv = {
            "userId": str(uid),
            "count": 20,
            "includePromotedContent": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items_with_cursor(op, kv, limit=limit)) as gen:
            async for rep, cursor in gen:
                for user in parse_users(rep.json(), limit):
                    yield user, cursor

    # verified_followers

    async def verified_followers_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_BlueVerifiedFollowers
        kv = {
            "userId": str(uid),
            "count": 20,
            "includePromotedContent": False,
            "withGrokTranslatedBio": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def verified_followers(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(self.verified_followers_raw(uid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_users(rep.json(), limit):
                    yield x

    # following

    async def following_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_Following
        kv = {
            "userId": str(uid),
            "count": 20,
            "includePromotedContent": False,
            "withGrokTranslatedBio": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def following(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(self.following_raw(uid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_users(rep.json(), limit):
                    yield x

    async def following_with_cursor(self, uid: int, limit=-1, kv: KV = None):
        """
        Same as following but yields (user, cursor) tuples.

        The cursor can be saved and passed back in kv={'cursor': saved_cursor}
        to resume pagination from where you left off.

        Example:
            last_cursor = None
            async for user, cursor in api.following_with_cursor(user_id, limit=500):
                print(user.username)
                last_cursor = cursor
            # Save last_cursor to database for resumption
        """
        op = OP_Following
        kv = {
            "userId": str(uid),
            "count": 20,
            "includePromotedContent": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items_with_cursor(op, kv, limit=limit)) as gen:
            async for rep, cursor in gen:
                for user in parse_users(rep.json(), limit):
                    yield user, cursor

    # subscriptions

    async def subscriptions_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserCreatorSubscriptions
        kv = {"userId": str(uid), "count": 20, "includePromotedContent": False, **(kv or {})}
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def subscriptions(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(self.subscriptions_raw(uid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_users(rep.json(), limit):
                    yield x

    # retweeters

    async def retweeters_raw(self, twid: int, limit=-1, kv: KV = None):
        op = OP_Retweeters
        kv = {
            "tweetId": str(twid),
            "count": 20,
            "enableRanking": True,
            "includePromotedContent": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def retweeters(self, twid: int, limit=-1, kv: KV = None):
        async with aclosing(self.retweeters_raw(twid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_users(rep.json(), limit):
                    yield x

    # user_tweets

    async def user_tweets_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserTweets
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def user_tweets(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(self.user_tweets_raw(uid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep.json(), limit):
                    yield x

    async def user_tweets_with_cursor(self, uid: int, limit=-1, kv: KV = None):
        """
        Same as user_tweets but yields (tweet, cursor) tuples for resumable pagination.

        The cursor can be saved and passed back in kv={'cursor': saved_cursor}
        to resume pagination from where you left off.

        Example:
            last_cursor = None
            async for tweet, cursor in api.user_tweets_with_cursor(user_id, limit=1000):
                print(tweet.rawContent)
                last_cursor = cursor
            # Save last_cursor to database for resumption
        """
        op = OP_UserTweets
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items_with_cursor(op, kv, limit=limit)) as gen:
            async for rep, cursor in gen:
                for tweet in parse_tweets(rep.json(), limit):
                    yield tweet, cursor

    # user_tweets_and_replies

    async def user_tweets_and_replies_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserTweetsAndReplies
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": True,
            "withCommunity": True,
            "withVoice": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def user_tweets_and_replies(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(self.user_tweets_and_replies_raw(uid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep.json(), limit):
                    yield x

    async def user_tweets_and_replies_with_cursor(self, uid: int, limit=-1, kv: KV = None):
        """
        Same as user_tweets_and_replies but yields (tweet, cursor) tuples.

        The cursor can be saved and passed back in kv={'cursor': saved_cursor}
        to resume pagination from where you left off.

        Example:
            last_cursor = None
            async for tweet, cursor in api.user_tweets_and_replies_with_cursor(user_id, limit=100):
                print(tweet.rawContent)
                last_cursor = cursor
            # Save last_cursor to database for resumption
        """
        op = OP_UserTweetsAndReplies
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": True,
            "withCommunity": True,
            "withVoice": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items_with_cursor(op, kv, limit=limit)) as gen:
            async for rep, cursor in gen:
                for tweet in parse_tweets(rep.json(), limit):
                    yield tweet, cursor

    # user_media

    async def user_media_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserMedia
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": False,
            "withClientEventToken": False,
            "withBirdwatchNotes": False,
            "withVoice": True,
            **(kv or {}),
        }

        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def user_media(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(self.user_media_raw(uid, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep, limit):
                    # sometimes some tweets without media, so skip them
                    media_count = (
                        len(x.media.photos) + len(x.media.videos) + len(x.media.animated)
                        if x.media
                        else 0
                    )

                    if media_count > 0:
                        yield x

    # list_timeline

    async def list_timeline_raw(self, list_id: int, limit=-1, kv: KV = None):
        op = OP_ListLatestTweetsTimeline
        kv = {"listId": str(list_id), "count": 20, **(kv or {})}
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def list_timeline(self, list_id: int, limit=-1, kv: KV = None):
        async with aclosing(self.list_timeline_raw(list_id, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep, limit):
                    yield x

    # community_timeline

    async def community_timeline_raw(self, community_id: str, limit=-1, kv: KV = None):
        op = OP_CommunityTweetsTimeline
        kv = {
            "communityId": str(community_id),
            "count": 20,
            "displayLocation": "Community",
            "rankingMode": "Relevance",
            "withCommunity": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def community_timeline(self, community_id: str, limit=-1, kv: KV = None):
        async with aclosing(self.community_timeline_raw(community_id, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep, limit):
                    yield x

    async def community_timeline_with_cursor(self, community_id: str, limit=-1, kv: KV = None):
        """
        Same as community_timeline but yields (tweet, cursor) tuples for resumable pagination.

        The cursor can be saved and passed back in kv={'cursor': saved_cursor}
        to resume pagination from where you left off.

        Example:
            last_cursor = None
            async for tweet, cursor in api.community_timeline_with_cursor(community_id, limit=1000):
                print(tweet.rawContent)
                last_cursor = cursor
            # Save last_cursor to database for resumption
        """
        op = OP_CommunityTweetsTimeline
        kv = {
            "communityId": str(community_id),
            "count": 20,
            "displayLocation": "Community",
            "rankingMode": "Relevance",
            "withCommunity": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items_with_cursor(op, kv, limit=limit)) as gen:
            async for rep, cursor in gen:
                for tweet in parse_tweets(rep, limit):
                    yield tweet, cursor

    # community_media_timeline

    async def community_media_timeline_raw(self, community_id: str, limit=-1, kv: KV = None):
        op = OP_CommunityMediaTimeline
        kv = {
            "communityId": str(community_id),
            "count": 20,
            "withCommunity": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def community_media_timeline(self, community_id: str, limit=-1, kv: KV = None):
        async with aclosing(self.community_media_timeline_raw(community_id, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep, limit):
                    yield x

    async def community_media_timeline_with_cursor(self, community_id: str, limit=-1, kv: KV = None):
        """
        Same as community_media_timeline but yields (tweet, cursor) tuples for resumable pagination.

        The cursor can be saved and passed back in kv={'cursor': saved_cursor}
        to resume pagination from where you left off.

        Example:
            last_cursor = None
            async for tweet, cursor in api.community_media_timeline_with_cursor(community_id, limit=1000):
                print(tweet.rawContent)
                last_cursor = cursor
            # Save last_cursor to database for resumption
        """
        op = OP_CommunityMediaTimeline
        kv = {
            "communityId": str(community_id),
            "count": 20,
            "withCommunity": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items_with_cursor(op, kv, limit=limit)) as gen:
            async for rep, cursor in gen:
                for tweet in parse_tweets(rep, limit):
                    yield tweet, cursor

    # community_members

    async def community_members_raw(self, community_id: str, kv: KV = None):
        op = OP_membersSliceTimeline_Query
        kv = {"communityId": str(community_id), **(kv or {})}
        return await self._gql_item(op, kv)

    async def community_members(self, community_id: str, kv: KV = None):
        """Returns community members data. Use parse_users on the result if needed."""
        return await self.community_members_raw(community_id, kv=kv)

    # audio_space

    async def audio_space_raw(self, space_id: str, kv: KV = None):
        op = OP_AudioSpaceById
        kv = {
            "id": space_id,
            "isMetatagsQuery": False,
            "withReplays": True,
            "withListeners": True,
            **(kv or {}),
        }
        ft = {
            "spaces_2022_h2_spaces_communities": True,
            "spaces_2022_h2_clipping": True,
        }
        return await self._gql_item(op, kv, ft)

    async def audio_space(self, space_id: str, kv: KV = None):
        """Returns raw audio space data (no parsing implemented yet)"""
        return await self.audio_space_raw(space_id, kv=kv)

    # about_account

    async def about_account_raw(self, screen_name: str, kv: KV = None):
        op = OP_AboutAccountQuery
        kv = {"screenName": screen_name, **(kv or {})}
        return await self._gql_item(op, kv)

    async def about_account(self, screen_name: str, kv: KV = None):
        """Returns raw account about data (no parsing implemented yet)"""
        return await self.about_account_raw(screen_name, kv=kv)

    # user_recommendations (REST API)

    async def user_recommendations_raw(self, uid: int, limit: int = 20, kv: KV = None):
        """
        Get recommended users to follow based on a given user profile.

        This uses Twitter's REST API endpoint for "Who to follow" recommendations.

        Args:
            uid: The user ID to get recommendations for
            limit: Maximum number of recommendations to return (default 20)
            kv: Additional parameters to merge

        Returns:
            Raw API response
        """
        params = {
            "include_profile_interstitial_type": 1,
            "include_blocking": 1,
            "include_blocked_by": 1,
            "include_followed_by": 1,
            "include_want_retweets": 1,
            "include_mute_edge": 1,
            "include_can_dm": 1,
            "include_can_media_tag": 1,
            "include_ext_is_blue_verified": 1,
            "include_ext_verified_type": 1,
            "include_ext_profile_image_shape": 1,
            "skip_status": 1,
            "pc": "true",
            "display_location": "profile-cluster-follow",
            "limit": limit,
            "user_id": uid,
            **(kv or {}),
        }
        return await self._rest_get("users/recommendations.json", params, queue="UserRecommendations")

    async def user_recommendations(self, uid: int, limit: int = 20, kv: KV = None):
        """
        Get recommended users to follow based on a given user profile.

        This uses Twitter's REST API endpoint for "Who to follow" recommendations.

        Args:
            uid: The user ID to get recommendations for
            limit: Maximum number of recommendations to return (default 20)
            kv: Additional parameters to merge

        Yields:
            User objects for each recommended user
        """
        rep = await self.user_recommendations_raw(uid, limit=limit, kv=kv)
        if rep is None:
            return

        data = rep.json()
        # REST API returns a list of user objects directly
        if isinstance(data, list):
            for user_obj in data:
                # The user data may be nested under 'user' key
                user_data = user_obj.get("user", user_obj)
                if user_data and "id_str" in user_data:
                    try:
                        yield User.parse(user_data)
                    except Exception:
                        continue

    # profile_spotlights

    async def profile_spotlights_raw(self, screen_name: str, kv: KV = None):
        """Get profile spotlight modules (communities, etc.) for a user."""
        op = OP_ProfileSpotlightsQuery
        kv = {"screen_name": screen_name, **(kv or {})}
        return await self._gql_item(op, kv)

    async def profile_spotlights(self, screen_name: str, kv: KV = None):
        """
        Returns profile spotlight data including communities the user has spotlighted.

        Returns dict with:
        - communities: List of community dicts with id, name, member_count, description
        """
        result = await self.profile_spotlights_raw(screen_name, kv=kv)
        if hasattr(result, 'json'):
            data = result.json()
        else:
            data = result

        # Parse communities from profile modules
        communities = []
        user_result = data.get('data', {}).get('user_result_by_screen_name', {}).get('result', {})
        modules = user_result.get('profilemodules', {}).get('v1', [])

        for module in modules:
            pm = module.get('profile_module', {})
            if pm.get('__typename') == 'CommunitiesModule':
                config = pm.get('config', {})
                community = config.get('community_results', {}).get('result', {})
                if community:
                    communities.append({
                        'id': int(community.get('rest_id', 0)),
                        'id_str': community.get('rest_id'),
                        'name': community.get('name'),
                        'description': community.get('description'),
                        'member_count': community.get('member_count'),
                    })

        return {'communities': communities}

    # trends

    async def trends_raw(self, trend_id: TrendId, limit=-1, kv: KV = None):
        map = {
            "trending": "VGltZWxpbmU6DAC2CwABAAAACHRyZW5kaW5nAAA",
            "news": "VGltZWxpbmU6DAC2CwABAAAABG5ld3MAAA",
            "sport": "VGltZWxpbmU6DAC2CwABAAAABnNwb3J0cwAA",
            "entertainment": "VGltZWxpbmU6DAC2CwABAAAADWVudGVydGFpbm1lbnQAAA",
        }
        trend_id = map.get(trend_id, trend_id)

        op = OP_GenericTimelineById
        kv = {
            "timelineId": trend_id,
            "count": 20,
            "withQuickPromoteEligibilityTweetFields": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def trends(self, trend_id: TrendId, limit=-1, kv: KV = None):
        async with aclosing(self.trends_raw(trend_id, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_trends(rep, limit):
                    yield x

    async def search_trend(self, q: str, limit=-1, kv: KV = None):
        kv = {
            "querySource": "trend_click",
            **(kv or {}),
        }
        async with aclosing(self.search_raw(q, limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep.json(), limit):
                    yield x

    # Get current user bookmarks

    async def bookmarks_raw(self, limit=-1, kv: KV = None):
        op = OP_Bookmarks
        kv = {
            "count": 20,
            "includePromotedContent": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def bookmarks(self, limit=-1, kv: KV = None):
        async with aclosing(self.bookmarks_raw(limit=limit, kv=kv)) as gen:
            async for rep in gen:
                for x in parse_tweets(rep.json(), limit):
                    yield x

    # create_tweet

    async def create_tweet_raw(
        self,
        text: str,
        *,
        media_ids: list[str] | None = None,
        reply_to_tweet_id: int | None = None,
        quote_tweet_id: int | None = None,
        possibly_sensitive: bool = False,
        kv: KV = None,
    ):
        """
        Create a tweet.

        Args:
            text: The text content of the tweet
            media_ids: List of media IDs to attach (from media upload endpoint)
            reply_to_tweet_id: Tweet ID to reply to
            quote_tweet_id: Tweet ID to quote
            possibly_sensitive: Mark tweet as containing sensitive content
            kv: Additional variables to merge

        Returns:
            Raw API response
        """
        op = OP_CreateTweet
        kv = {
            "tweet_text": text,
            "dark_request": False,
            "media": {
                "media_entities": [{"media_id": mid, "tagged_users": []} for mid in (media_ids or [])],
                "possibly_sensitive": possibly_sensitive,
            },
            "semantic_annotation_ids": [],
            "broadcast": False,
            "disallowed_reply_options": None,
            **(kv or {}),
        }

        # Handle reply
        if reply_to_tweet_id is not None:
            kv["reply"] = {
                "in_reply_to_tweet_id": str(reply_to_tweet_id),
                "exclude_reply_user_ids": [],
            }

        # Handle quote tweet
        if quote_tweet_id is not None:
            kv["quote_tweet_id"] = str(quote_tweet_id)

        return await self._gql_mutation(op, kv)

    async def create_tweet(
        self,
        text: str,
        *,
        media_ids: list[str] | None = None,
        reply_to_tweet_id: int | None = None,
        quote_tweet_id: int | None = None,
        possibly_sensitive: bool = False,
        kv: KV = None,
    ) -> Tweet | None:
        """
        Create a tweet and return the created Tweet object.

        Args:
            text: The text content of the tweet
            media_ids: List of media IDs to attach (from media upload endpoint)
            reply_to_tweet_id: Tweet ID to reply to
            quote_tweet_id: Tweet ID to quote
            possibly_sensitive: Mark tweet as containing sensitive content
            kv: Additional variables to merge

        Returns:
            Tweet object if successful, None otherwise
        """
        rep = await self.create_tweet_raw(
            text,
            media_ids=media_ids,
            reply_to_tweet_id=reply_to_tweet_id,
            quote_tweet_id=quote_tweet_id,
            possibly_sensitive=possibly_sensitive,
            kv=kv,
        )
        if rep is None:
            return None

        data = rep.json()
        # Extract tweet from response
        tweet_result = get_by_path(data, "create_tweet")
        if tweet_result:
            tweet_results = tweet_result.get("tweet_results", {})
            if tweet_results:
                return parse_tweet({"data": {"tweetResult": tweet_results}})
        return None
