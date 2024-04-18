// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package functionUtil

import "math"

type FixedFloat64Convert struct {
    pow, maxValue, minValue float64
}

func NewFixedFloat64Convert(scale, width int) (c FixedFloat64Convert, isFixedFloat64 bool) {
    isFixedFloat64 = scale >= 0 && width != 0
    if isFixedFloat64 {
        c.pow = math.Pow10(scale)
        c.maxValue = math.Pow10(width - scale) - 1.0/c.pow
        c.minValue = -c.maxValue
    }
    return c, false
}

func (c FixedFloat64Convert) Convert(v float64) (r float64, outOfRange bool) {
    value := math.Floor(v) + math.Round((v-math.Floor(v))*c.pow)/c.pow
    if value > c.maxValue || value < c.minValue {
        outOfRange = true
    }
    return value, outOfRange
}