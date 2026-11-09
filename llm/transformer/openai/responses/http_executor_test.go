package responses

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/llm/auth"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
)

type httpTransportCaptureExecutor struct {
	request *httpclient.Request
}

func (e *httpTransportCaptureExecutor) Do(_ context.Context, request *httpclient.Request) (*httpclient.Response, error) {
	e.request = request
	return &httpclient.Response{StatusCode: http.StatusOK}, nil
}

func (e *httpTransportCaptureExecutor) DoStream(_ context.Context, request *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
	e.request = request
	return streams.SliceStream([]*httpclient.StreamEvent{}), nil
}

func TestHTTPTransportExecutorStripsWebSocketMetadataAndKeepsContinuation(t *testing.T) {
	outbound, err := NewOutboundTransformerWithConfig(&Config{
		BaseURL:        "https://api.example.com/v1",
		APIKeyProvider: auth.NewStaticKeyProvider("test-key"),
		Transport:      TransportHTTP,
	})
	require.NoError(t, err)

	inner := &httpTransportCaptureExecutor{}
	executor := outbound.CustomizeExecutor(inner)
	request := &httpclient.Request{
		Headers: http.Header{
			"Openai-Beta": {WebSocketBetaHeaderValue + ", other_beta=v1"},
		},
		Body: []byte(`{"model":"gpt-5","previous_response_id":"resp_ws","stream":true,"input":"hello"}`),
	}

	stream, err := executor.DoStream(context.Background(), request)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.NotNil(t, inner.request)
	require.Equal(t, "other_beta=v1", inner.request.Headers.Get("OpenAI-Beta"))
	require.Equal(t, "resp_ws", gjson.GetBytes(inner.request.Body, "previous_response_id").String())
	require.True(t, gjson.GetBytes(inner.request.Body, "stream").Bool())
	require.Equal(t, "hello", gjson.GetBytes(inner.request.Body, "input").String())

	// Transport cleanup must not mutate the request retained by the pipeline.
	require.Equal(t, WebSocketBetaHeaderValue+", other_beta=v1", request.Headers.Get("OpenAI-Beta"))
	require.Equal(t, "resp_ws", gjson.GetBytes(request.Body, "previous_response_id").String())
}

func TestPrepareHTTPTransportRequestKeepsContinuationWithoutInput(t *testing.T) {
	request := &httpclient.Request{
		Headers: http.Header{"Openai-Beta": {WebSocketBetaHeaderValue}},
		Body:    []byte(`{"model":"gpt-5","previous_response_id":"resp_ws","stream":true}`),
	}

	prepared := PrepareHTTPTransportRequest(request, false)
	require.Empty(t, prepared.Headers.Get("OpenAI-Beta"))
	require.Equal(t, "resp_ws", gjson.GetBytes(prepared.Body, "previous_response_id").String())
	require.False(t, gjson.GetBytes(prepared.Body, "input").Exists())
}

func TestPrepareHTTPTransportRequestConvertsOnlyUncalledAppPageContext(t *testing.T) {
	body := `{"model":"gpt-6.1-sol","input":[
		{"type":"message","role":"user","content":"before"},
		{"type":"function_call_output","id":"fco_1","namespace":"agent","name":"codex_apps_open_page","output":"{\"page_id\":\"first\"}"},
		{"type":"function_call_output","name":"codex_apps_selected_page_part_1_of_2","call_id":null,"output":"selected\n\"text\""},
		{"type":"function_call_output","name":"codex_apps_selected_page_part_2_of_2","output":[{"type":"input_text","text":"more selected text"}]},
		{"type":"message","role":"assistant","content":"between"},
		{"type":"function_call_output","name":"codex_apps_open_page","call_id":"","output":"{\"page_id\":\"second\"}"},
		{"type":"function_call_output","name":"codex_apps_open_page","call_id":"call_real","output":"real tool result"},
		{"type":"function_call_output","name":"another_tool","output":"unknown context"},
		{"type":"custom_tool_call_output","name":"codex_apps_open_page","output":"custom result"},
		{"type":"function_call_output","name":"codex_apps_open_page","output":{"invalid":"output"}}
	]}`
	request := &httpclient.Request{Body: []byte(body)}

	prepared := PrepareHTTPTransportRequest(request, false)
	require.NotSame(t, request, prepared)
	require.JSONEq(t, `{"model":"gpt-6.1-sol","input":[
		{"type":"message","role":"user","content":"before"},
		{"type":"message","role":"user","content":"<external_codex_apps_open_page>\n{\"page_id\":\"first\"}\n</external_codex_apps_open_page>"},
		{"type":"message","role":"user","content":"<external_codex_apps_selected_page_part_1_of_2>\nselected\n\"text\"\n</external_codex_apps_selected_page_part_1_of_2>"},
		{"type":"function_call_output","name":"codex_apps_selected_page_part_2_of_2","output":[{"type":"input_text","text":"more selected text"}]},
		{"type":"message","role":"assistant","content":"between"},
		{"type":"message","role":"user","content":"<external_codex_apps_open_page>\n{\"page_id\":\"second\"}\n</external_codex_apps_open_page>"},
		{"type":"function_call_output","name":"codex_apps_open_page","call_id":"call_real","output":"real tool result"},
		{"type":"function_call_output","name":"another_tool","output":"unknown context"},
		{"type":"custom_tool_call_output","name":"codex_apps_open_page","output":"custom result"},
		{"type":"function_call_output","name":"codex_apps_open_page","output":{"invalid":"output"}}
	]}`, string(prepared.Body))
	require.Equal(t, body, string(request.Body), "transport preparation must not mutate history")
	require.Same(t, prepared, PrepareHTTPTransportRequest(prepared, false))
	invalidInput := &httpclient.Request{Body: []byte(`{"input":{"type":"function_call_output","name":"codex_apps_open_page","output":"invalid input"}}`)}
	require.Same(t, invalidInput, PrepareHTTPTransportRequest(invalidInput, false))

	outbound, err := NewOutboundTransformerWithConfig(&Config{
		BaseURL:        "https://api.example.com/v1",
		APIKeyProvider: auth.NewStaticKeyProvider("test-key"),
		Transport:      TransportWebSocket,
	})
	require.NoError(t, err)
	require.Same(t, request, outbound.FinalizeTransportRequest(request))
}

func TestHTTPTransportExecutorKeepsNormalHTTPContinuation(t *testing.T) {
	outbound, err := NewOutboundTransformerWithConfig(&Config{
		BaseURL:        "https://api.example.com/v1",
		APIKeyProvider: auth.NewStaticKeyProvider("test-key"),
	})
	require.NoError(t, err)

	inner := &httpTransportCaptureExecutor{}
	executor := outbound.CustomizeExecutor(inner)
	request := &httpclient.Request{
		Headers: http.Header{"Openai-Beta": {"other_beta=v1"}},
		Body:    []byte(`{"model":"gpt-5","previous_response_id":"resp_http","stream":true}`),
	}

	stream, err := executor.DoStream(context.Background(), request)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Same(t, request, inner.request)
	require.Equal(t, "resp_http", gjson.GetBytes(inner.request.Body, "previous_response_id").String())
	require.Equal(t, "other_beta=v1", inner.request.Headers.Get("OpenAI-Beta"))
}
