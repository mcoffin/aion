package timedb

import (
    "bytes"
    "fmt"
)

type InputPoint struct {
    Value float32
    Tags map[string]string
}

func (p *InputPoint) seriesID() string {
    var buffer bytes.Buffer
    for k, v := range p.Tags {
        buffer.WriteString(fmt.Sprintf("%s=%s|", k, v))
    }
    return buffer.String()
}
