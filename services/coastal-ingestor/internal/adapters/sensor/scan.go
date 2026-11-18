package sensor

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

// page is one /v1/observations response after structural splitting.
type page struct {
	// generated is generated_at, zero when absent or unusable.
	generated time.Time
	// receivedAt is when the response finished arriving.
	receivedAt time.Time
	items      []rawItem
	// next is the cursor to request next; hasNext is false at the tail.
	next    string
	hasNext bool
	// damaged marks a response whose structure is incomplete or untrustworthy
	// (cut short, unbalanced, too many items, unusable next_cursor). Its
	// items are salvage and it never advances the cursor.
	damaged bool
}

// rawItem is the bytes of one element of "items", not yet validated.
type rawItem struct {
	data []byte
	// violation is preset when the splitter already knows the element cannot
	// be a reading (oversized, or "items" itself was not an array).
	violation domain.QuarantineReason
}

// decodePage splits a response body into its envelope fields and item spans.
//
// It deliberately does not use json.Unmarshal on the whole body: that rejects
// the entire response for a single syntax error anywhere in it (an unquoted
// word, a NaN, one item cut in half), which would hide every good reading on
// the page and pin the cursor in front of it forever. Instead the body is
// walked tracking only strings and bracket depth, which is enough to find
// where each top-level field and each array element starts and ends even when
// its content is not valid JSON. Elements are validated one by one afterwards.
//
// Outcomes:
//   - body is not an object at all: no page, ErrSourcePayload;
//   - structure complete: page with nil error (broken elements are the
//     reader's problem, not the page's);
//   - structure incomplete: page with damaged=true, the elements that were
//     complete before the damage, and ErrSourcePayload.
func decodePage(body []byte) (page, error) {
	var result page
	var generatedRaw, nextRaw []byte
	i := skipSpace(body, 0)
	if i >= len(body) || body[i] != '{' {
		return page{}, fmt.Errorf("%w: observations: response is not a JSON object", ports.ErrSourcePayload)
	}
	i++
	damage := ""
fields:
	for {
		i = skipSpace(body, i)
		if i < len(body) && body[i] == '}' {
			break
		}
		if i >= len(body) || body[i] != '"' {
			damage = "object key expected"
			break
		}
		keyEnd, ok := scanValue(body, i)
		if !ok {
			damage = "response cut inside a key"
			break
		}
		var key string
		if json.Unmarshal(body[i:keyEnd], &key) != nil {
			damage = "unreadable object key"
			break
		}
		i = skipSpace(body, keyEnd)
		if i >= len(body) || body[i] != ':' {
			damage = "':' expected after key"
			break
		}
		i = skipSpace(body, i+1)
		if key == "items" && i < len(body) && body[i] == '[' {
			// Duplicate "items" keys: the last one wins, like encoding/json.
			var complete bool
			result.items, i, complete = splitItems(body, i+1)
			if len(result.items) > maxItemsPerPage {
				result.items = result.items[:maxItemsPerPage]
				damage = fmt.Sprintf("more than %d items", maxItemsPerPage)
				break
			}
			if !complete {
				damage = "items array cut short or unbalanced"
				break
			}
		} else {
			end, ok := scanValue(body, i)
			if !ok {
				damage = "response cut inside field " + key
				break
			}
			value := body[i:end]
			switch key {
			case "generated_at":
				generatedRaw = value
			case "next_cursor":
				nextRaw = value
			case "items":
				result.items = nil
				if string(value) != "null" {
					// "items" is an object, string, number: the page has no
					// readable observations. Set the value aside as one
					// malformed item so the violation is on record.
					result.items = []rawItem{boundItem(value, domain.ReasonMalformedItem)}
				}
			}
			i = end
		}
		i = skipSpace(body, i)
		switch {
		case i < len(body) && body[i] == ',':
			i++
		case i < len(body) && body[i] == '}':
			break fields
		default:
			damage = "',' or '}' expected"
			break fields
		}
	}

	var generated string
	if json.Unmarshal(generatedRaw, &generated) == nil {
		if parsed, err := time.Parse(time.RFC3339, generated); err == nil {
			result.generated = parsed
		}
	}
	if damage != "" {
		result.damaged = true
		return result, fmt.Errorf("%w: observations: %s", ports.ErrSourcePayload, damage)
	}
	switch {
	case nextRaw == nil || string(nextRaw) == "null":
	case nextRaw[0] == '"':
		if json.Unmarshal(nextRaw, &result.next) != nil {
			result.damaged = true
			return result, fmt.Errorf("%w: observations: unreadable next_cursor", ports.ErrSourcePayload)
		}
		result.hasNext = result.next != ""
	default:
		result.damaged = true
		return result, fmt.Errorf("%w: observations: next_cursor is not a string", ports.ErrSourcePayload)
	}
	return result, nil
}

// splitItems walks the elements of an array whose '[' precedes position i. It
// returns the complete elements, the position after the closing ']', and
// whether the array was closed properly. It stops collecting once
// maxItemsPerPage is exceeded.
func splitItems(body []byte, i int) ([]rawItem, int, bool) {
	var items []rawItem
	for {
		i = skipSpace(body, i)
		if i < len(body) && body[i] == ']' {
			return items, i + 1, true
		}
		end, ok := scanValue(body, i)
		if !ok {
			return items, len(body), false
		}
		items = append(items, boundItem(body[i:end], ""))
		if len(items) > maxItemsPerPage {
			return items, end, false
		}
		i = skipSpace(body, end)
		switch {
		case i < len(body) && body[i] == ',':
			i++
		case i < len(body) && body[i] == ']':
			return items, i + 1, true
		default:
			return items, len(body), false
		}
	}
}

// boundItem copies an element out of the response body (so the body can be
// released) and applies the per-item size cap.
func boundItem(value []byte, violation domain.QuarantineReason) rawItem {
	if len(value) > maxItemBytes {
		return rawItem{data: append([]byte(nil), value[:oversizedPayloadBytes]...), violation: domain.ReasonOversizedItem}
	}
	return rawItem{data: append([]byte(nil), value...), violation: violation}
}

func skipSpace(body []byte, i int) int {
	for i < len(body) {
		switch body[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// scanValue returns the position after the value starting at i. Only string
// quoting and bracket depth are tracked, iteratively, so arbitrarily deep or
// invalid content can neither be misread as a boundary nor exhaust the stack.
// ok is false when the input ends before the value does, or there is no value.
func scanValue(body []byte, i int) (int, bool) {
	if i >= len(body) {
		return i, false
	}
	switch body[i] {
	case '"':
		return scanString(body, i)
	case '{', '[':
		depth := 0
		for j := i; j < len(body); j++ {
			switch body[j] {
			case '"':
				end, ok := scanString(body, j)
				if !ok {
					return len(body), false
				}
				j = end - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return j + 1, true
				}
			}
		}
		return len(body), false
	default:
		// A scalar (number, true, null, or garbage such as NaN) runs to the
		// next delimiter; reaching the end of input means it may be cut.
		j := i
		for j < len(body) {
			switch body[j] {
			case ',', ']', '}', ' ', '\t', '\n', '\r':
				return j, j > i
			}
			j++
		}
		return j, false
	}
}

func scanString(body []byte, i int) (int, bool) {
	for j := i + 1; j < len(body); j++ {
		switch body[j] {
		case '\\':
			j++
		case '"':
			return j + 1, true
		}
	}
	return len(body), false
}
