export interface Route {
  channelID: string;
  directory: string;
}

export interface AccessPolicy {
  teamID: string;
  appID: string;
  users: readonly string[];
  channels: readonly Route[];
  maxInputChars: number;
}

export interface Origin {
  teamID: string;
  channelID: string;
  threadTS: string;
  userID: string;
}

export interface Inbound extends Origin {
  eventID: string;
  messageTS: string;
  directory: string;
  text: string;
  delivery: "queue" | "steer";
}

export function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined;
}

export const timestamp = (value: unknown): value is string =>
  typeof value === "string" && /^\d{10,16}\.\d{6}$/.test(value);

export function permitted(origin: Origin, policy: AccessPolicy, directory?: string): boolean {
  return origin.teamID === policy.teamID && policy.users.includes(origin.userID) &&
    policy.channels.some(route => route.channelID === origin.channelID &&
      (directory === undefined || route.directory === directory));
}

export function parseInbound(body: unknown, policy: AccessPolicy, botUserID: string): Inbound | undefined {
  const b = record(body);
  const event = record(b?.event);
  if (!b || !event || b.type !== "event_callback" || b.team_id !== policy.teamID || b.api_app_id !== policy.appID ||
      typeof b.event_id !== "string" || !/^Ev[A-Za-z0-9_-]{1,160}$/.test(b.event_id) ||
      event.type !== "app_mention" || typeof event.user !== "string" || event.user === botUserID ||
      event.bot_id !== undefined || event.app_id !== undefined || event.subtype !== undefined || event.hidden === true ||
      (event.team !== undefined && event.team !== policy.teamID) ||
      (event.user_team !== undefined && event.user_team !== policy.teamID) ||
      typeof event.channel !== "string" || !timestamp(event.ts) ||
      (event.thread_ts !== undefined && !timestamp(event.thread_ts)) || typeof event.text !== "string") return;
  const route = policy.channels.find(route => route.channelID === event.channel);
  if (!route || !policy.users.includes(event.user)) return;
  const mention = `<@${botUserID}>`;
  if (!event.text.trimStart().startsWith(mention)) return;
  let text = event.text.trimStart().slice(mention.length).trim();
  let delivery: Inbound["delivery"] = "queue";
  if (/^!steer(?:\s|$)/.test(text)) {
    delivery = "steer";
    text = text.slice(6).trim();
  }
  text = text.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");
  if (!text || text.length > policy.maxInputChars) return;
  return {
    teamID: policy.teamID, channelID: route.channelID, threadTS: (event.thread_ts as string | undefined) ?? event.ts,
    userID: event.user, eventID: b.event_id, messageTS: event.ts, directory: route.directory, text, delivery,
  };
}
