import assert from "node:assert/strict";
import test from "node:test";

import {
  appendGraphqlSnapshot,
  chromiumNetworkArgs,
  inspectGraphqlBody,
  isFacebookLoginRoute,
  parseBrowserNetworkMode,
  preferredExternalPostId,
  remoteAddressFamily,
  selectNewestDiscoveredPost,
} from "../src/post.ts";

const sourceUrl = "https://www.facebook.com/uth.example/";

test("browser network mode defaults safely and rejects unknown values", () => {
  assert.equal(parseBrowserNetworkMode(undefined), "system");
  assert.equal(parseBrowserNetworkMode(" prefer_ipv4 "), "prefer_ipv4");
  assert.throws(() => parseBrowserNetworkMode("ipv4_only"));
});

test("host resolver rule accepts only a valid IPv4 address", () => {
  assert.deepEqual(chromiumNetworkArgs(null), []);
  assert.deepEqual(chromiumNetworkArgs("2001:db8::1"), []);
  assert.deepEqual(chromiumNetworkArgs("not-an-address"), []);
  assert.deepEqual(chromiumNetworkArgs("203.0.113.10"), [
    "--disable-quic",
    "--host-resolver-rules=MAP www.facebook.com 203.0.113.10, EXCLUDE localhost",
  ]);
});

test("remote address family records no concrete address", () => {
  assert.equal(remoteAddressFamily("203.0.113.10"), "ipv4");
  assert.equal(remoteAddressFamily("2001:db8::1"), "ipv6");
  assert.equal(remoteAddressFamily("facebook.example"), "unknown");
  assert.equal(remoteAddressFamily(null), "unknown");
});

test("newer hydrated DOM post wins over stale GraphQL", () => {
  const graphql = {
    url: "https://www.facebook.com/example/posts/pfbidOld",
    text: "old",
    publishedAt: "2026-07-28T12:00:00.000Z",
    externalPostId: "pfbidOld",
    origin: "graphql",
  };
  const dom = {
    url: "https://www.facebook.com/example/posts/pfbidNew",
    text: "new",
    publishedAt: "2026-07-29T15:00:00.000Z",
    externalPostId: "pfbidNew",
    origin: "dom",
  };
  assert.deepEqual(selectNewestDiscoveredPost(graphql, dom), dom);
});

test("unresolved DOM post does not replace a complete GraphQL post", () => {
  const graphql = {
    url: "https://www.facebook.com/example/posts/pfbidKnown",
    text: "known",
    publishedAt: "2026-07-28T12:00:00.000Z",
    externalPostId: "pfbidKnown",
    origin: "graphql",
  };
  const dom = {
    url: "https://www.facebook.com/example/posts/pfbidUnknown",
    text: "unknown time",
    publishedAt: null,
    externalPostId: null,
    origin: "dom",
  };
  assert.deepEqual(selectNewestDiscoveredPost(graphql, dom), graphql);
});

test("same DOM and GraphQL post merge richer visible text with stable metadata", () => {
  const selected = selectNewestDiscoveredPost(
    {
      url: "https://www.facebook.com/example/posts/pfbidSame",
      text: "short",
      publishedAt: "2026-07-29T15:00:00.000Z",
      externalPostId: "122100000000000007",
      origin: "graphql",
    },
    {
      url: "https://www.facebook.com/example/posts/pfbidSame",
      text: "longer visible post text",
      publishedAt: null,
      externalPostId: null,
      origin: "dom",
    },
  );
  assert.equal(selected.text, "longer visible post text");
  assert.equal(selected.publishedAt, "2026-07-29T15:00:00.000Z");
  assert.equal(selected.externalPostId, "122100000000000007");
});

test("phân biệt trang đăng nhập hoàn chỉnh với hộp thoại trên trang công khai", () => {
  assert.equal(
    isFacebookLoginRoute(
      "https://www.facebook.com/login/?next=https%3A%2F%2Fwww.facebook.com%2Futh.example%2F",
    ),
    true,
  );
  assert.equal(
    isFacebookLoginRoute("https://www.facebook.com/checkpoint/1501092823525282/"),
    true,
  );
  assert.equal(isFacebookLoginRoute(sourceUrl), false);
  assert.equal(
    isFacebookLoginRoute(
      "https://www.facebook.com/plugins/page.php?href=https%3A%2F%2Fwww.facebook.com%2Futh.example%2F",
    ),
    false,
  );
});

test("ưu tiên numeric post ID ổn định hơn pfbid", () => {
  assert.equal(
    preferredExternalPostId(
      "pfbidRotatingPresentation",
      "122100000000000009",
    ),
    "122100000000000009",
  );
  assert.equal(
    preferredExternalPostId(null, "pfbidOnlyAvailableIdentity"),
    "pfbidOnlyAvailableIdentity",
  );
});

function story({
  id,
  owner = "uth.example",
  ownerId = "61566022178073",
  timestamp,
  text,
  url,
}) {
  return {
    story: {
      creation_time: timestamp,
      message: { text },
      owning_profile: {
        id: ownerId,
        username: owner,
        url: `https://www.facebook.com/${owner}/`,
      },
      feedback: {
        subscription_target_id: id,
      },
      url,
    },
  };
}

test("chọn bài mới nhất đúng owner từ GraphQL dạng nhiều dòng", () => {
  const older = story({
    id: "122100000000000001",
    timestamp: 1_751_328_000,
    text: "Thông báo cũ từ đúng fanpage.",
    url: "https://www.facebook.com/uth.example/posts/pfbidOlder",
  });
  const latest = story({
    id: "122100000000000002",
    timestamp: 1_751_414_400,
    text: "Thông báo mới nhất từ đúng fanpage.",
    url: "https://www.facebook.com/permalink.php?story_fbid=pfbidLatest&id=61566022178073&ref=share",
  });
  const foreign = story({
    id: "122100000000000003",
    owner: "another.page",
    ownerId: "61566022178074",
    timestamp: 1_751_500_800,
    text: "Bài mới hơn nhưng thuộc fanpage khác.",
    url: "https://www.facebook.com/another.page/posts/pfbidForeign",
  });
  const raw = [
    `for (;;);${JSON.stringify({ data: { node: older } })}`,
    JSON.stringify({ data: { node: latest } }),
    JSON.stringify({ data: { node: foreign } }),
  ].join("\n");

  const inspection = inspectGraphqlBody(raw, sourceUrl);

  assert.equal(inspection.posts.length, 2);
  assert.equal(inspection.posts[0].externalPostId, "122100000000000002");
  assert.equal(
    inspection.posts[0].url,
    "https://www.facebook.com/uth.example/posts/pfbidLatest",
  );
  assert.equal(inspection.posts[0].text, "Thông báo mới nhất từ đúng fanpage.");
  assert.equal(inspection.posts[0].publishedAt, "2025-07-02T00:00:00.000Z");
  const normalized = JSON.parse(
    /data-uth-source="graphql-normalized">([^<]+)<\/script>/.exec(
      appendGraphqlSnapshot("<html></html>", inspection.payloads, inspection.posts),
    )[1],
  );
  assert.equal(normalized.posts.length, 2);
});

test("tạo permalink có owner khi GraphQL không trả URL", () => {
  const raw = JSON.stringify({
    data: {
      node: story({
        id: "122100000000000004",
        timestamp: 1_751_414_400,
        text: "Thông báo chỉ có ID bài đăng.",
        url: null,
      }),
    },
  });

  const inspection = inspectGraphqlBody(raw, sourceUrl);

  assert.equal(inspection.posts.length, 1);
  assert.equal(
    inspection.posts[0].url,
    "https://www.facebook.com/uth.example/posts/122100000000000004",
  );
});

test("bỏ dòng lỗi và escape thẻ đóng script trong snapshot", () => {
  const raw = [
    "{malformed",
    JSON.stringify({
      data: {
        node: story({
          id: "122100000000000005",
          timestamp: 1_751_414_400,
          text: "</script><div>Nội dung vẫn là JSON hợp lệ.</div>",
          url: "https://www.facebook.com/uth.example/posts/pfbidEscaped",
        }),
      },
    }),
  ].join("\n");

  const inspection = inspectGraphqlBody(raw, sourceUrl);

  assert.equal(inspection.posts.length, 1);
  assert.equal(inspection.payloads.length, 1);
  assert.equal(inspection.payloads[0].includes("</script>"), false);
  assert.doesNotThrow(() => JSON.parse(inspection.payloads[0]));
});

test("ghi trường chuẩn hóa mà Rust parser sử dụng vào snapshot", () => {
  const raw = JSON.stringify({
    data: {
      node: story({
        id: "122100000000000006",
        timestamp: 1_751_414_400,
        text: "Thông báo được chuyển qua contract nội bộ.",
        url: "https://www.facebook.com/uth.example/posts/pfbidContract",
      }),
    },
  });
  const inspection = inspectGraphqlBody(raw, sourceUrl);

  const html = appendGraphqlSnapshot(
    "<html><body></body></html>",
    inspection.payloads,
    inspection.posts,
  );
  const match = html.match(
    /data-uth-source="graphql-normalized">([^<]+)<\/script>/,
  );

  assert.notEqual(match, null);
  const normalized = JSON.parse(match[1]);
  assert.deepEqual(normalized.posts[0], {
    post_id: "122100000000000006",
    creation_time: 1_751_414_400,
    message: { text: "Thông báo được chuyển qua contract nội bộ." },
    url: "https://www.facebook.com/uth.example/posts/pfbidContract",
  });
});
