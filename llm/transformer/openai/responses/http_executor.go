package responses

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
)

type httpTransportExecutor struct {
	inner    pipeline.Executor
	finalize func(*httpclient.Request) *httpclient.Request
}

func (e *httpTransportExecutor) Do(ctx context.Context, request *httpclient.Request) (*httpclient.Response, error) {
	return e.inner.Do(ctx, e.finalize(request))
}

func (e *httpTransportExecutor) DoStream(ctx context.Context, request *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
	return e.inner.DoStream(ctx, e.finalize(request))
}

// PrepareHTTPTransportRequest normalizes WebSocket-v2 metadata and App page context
// before an HTTP/SSE attempt. Generic Responses HTTP requests retain a normal
// previous_response_id. Codex callers can force removal because their HTTP
// endpoint does not support that field.
func PrepareHTTPTransportRequest(request *httpclient.Request, stripPreviousResponseID bool) *httpclient.Request {
	if request == nil {
		return nil
	}

	headers, webSocketBetaRemoved := withoutWebSocketBeta(request.Headers)
	body := request.Body
	bodyChanged := false
	if stripPreviousResponseID {
		body, bodyChanged = withoutPreviousResponseID(body)
	}
	input := gjson.GetBytes(body, "input")
	if input.IsArray() {
		for index, item := range input.Array() {
			name := item.Get("name").String()
			if item.Get("type").String() != "function_call_output" || item.Get("call_id").String() != "" ||
				(name != "codex_apps_open_page" && !strings.HasPrefix(name, "codex_apps_selected_page_part_")) {
				continue
			}
			output := item.Get("output")
			if output.Type != gjson.String {
				continue
			}

			// App page context is untrusted input, not the result of a model-requested tool call.
			message := map[string]string{
				"type":    "message",
				"role":    "user",
				"content": "<external_" + name + ">\n" + output.String() + "\n</external_" + name + ">",
			}
			updated, err := sjson.SetBytes(body, "input."+strconv.Itoa(index), message)
			if err != nil {
				continue
			}
			body = updated
			bodyChanged = true
		}
	}

	if !webSocketBetaRemoved && !bodyChanged {
		return request
	}

	cloned := *request
	if webSocketBetaRemoved {
		cloned.Headers = headers
	}
	if bodyChanged {
		cloned.Body = body
	}

	return &cloned
}

func withoutPreviousResponseID(body []byte) ([]byte, bool) {
	if !gjson.GetBytes(body, "previous_response_id").Exists() {
		return body, false
	}

	filtered, err := sjson.DeleteBytes(body, "previous_response_id")
	if err != nil {
		return body, false
	}

	return filtered, true
}

func withoutWebSocketBeta(headers http.Header) (http.Header, bool) {
	if headers == nil {
		return nil, false
	}

	values := headers.Values("OpenAI-Beta")
	if len(values) == 0 {
		return headers, false
	}

	filtered := make([]string, 0, len(values))
	changed := false
	for _, value := range values {
		parts := strings.Split(value, ",")
		kept := parts[:0]
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(strings.ToLower(part), "responses_websockets=") {
				changed = true
				continue
			}
			if part != "" {
				kept = append(kept, part)
			}
		}
		if len(kept) > 0 {
			filtered = append(filtered, strings.Join(kept, ", "))
		}
	}
	if !changed {
		return headers, false
	}

	cloned := headers.Clone()
	cloned.Del("OpenAI-Beta")
	for _, value := range filtered {
		cloned.Add("OpenAI-Beta", value)
	}

	return cloned, true
}
