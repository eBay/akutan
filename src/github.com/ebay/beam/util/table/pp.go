// Copyright 2019 eBay Inc.
// Primary authors: Simon Fell, Diego Ongaro,
//                  Raymond Kroeker, and Sathish Kandasamy.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package table formats data into a text-based table for human consumption.
package table

import (
	"bufio"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/ebay/beam/util/cmp"
	"golang.org/x/text/unicode/norm"
)

// Options represents different ways to control how the table is generated
type Options int

const (
	// HeaderRow if specified will format the first row in the table
	// as a header (i.e. there is a separator between it and the next row)
	HeaderRow Options = 1 << iota
	// FooterRow if specified will format the last row of the table
	// as a footer (i.e. there is a separator between it and the previous row)
	FooterRow
	// SkipEmpty if specified will cause nothing to be generated in the case
	// that the table has no data (i.e. no rows besides the header & footer rows
	// if they are enabled)
	SkipEmpty
	// RightJustify indicates that cells should have their contents right
	// justified (left padded), rather than the default of left justified.
	RightJustify
)

func (o Options) numberOfChromeRows() int {
	r := 0
	if o.hasHeaderRow() {
		r++
	}
	if o.hasFooterRow() {
		r++
	}
	return r
}

func (o Options) skipEmpty() bool {
	return o&SkipEmpty != 0
}

func (o Options) hasHeaderRow() bool {
	return o&HeaderRow != 0
}

func (o Options) hasFooterRow() bool {
	return o&FooterRow != 0
}

// PrettyPrint writes 't' as a nicely formatted table to the supplied Writer.
//
// if HeaderRow / FooterRow options are specified then a divider will be added
// between the header and/or footer rows. Cells are allowed to be multi-line,
// use \n as a line break. If no Justify option is used, the default is to left
// justify.
func PrettyPrint(dest io.Writer, t [][]string, opts Options) {
	if len(t) == 0 || (opts.skipEmpty() && len(t) <= opts.numberOfChromeRows()) {
		return
	}
	w := bufio.NewWriterSize(dest, 256)
	defer w.Flush()
	table := make([][]cell, len(t))
	for ridx, row := range t {
		table[ridx] = make([]cell, len(row))
		for cidx, c := range row {
			table[ridx][cidx] = makeCell(c)
		}
	}
	for cidx := range table[0] {
		maxWidth := 0
		for ridx := range table {
			maxWidth = cmp.MaxInt(maxWidth, table[ridx][cidx].Width())
		}
		for ridx := range table {
			c := &table[ridx][cidx]
			c.pad(opts, c.Height(), maxWidth)
		}
	}
	divider := func() {
		for _, c := range table[0] {
			io.WriteString(w, " ")
			for i := 0; i < c.Width(); i++ {
				io.WriteString(w, "-")
			}
			io.WriteString(w, " |")
		}
		io.WriteString(w, "\n")
	}
	for ridx, r := range table {
		maxHeight := 0
		for cidx := range r {
			maxHeight = cmp.MaxInt(maxHeight, r[cidx].Height())
		}
		for cidx := range r {
			r[cidx].pad(opts, maxHeight, r[cidx].Width())
		}
		for lidx := 0; lidx < maxHeight; lidx++ {
			for cidx := range r {
				io.WriteString(w, " ")
				io.WriteString(w, r[cidx].lines[lidx])
				io.WriteString(w, " |")
			}
			io.WriteString(w, "\n")
		}
		if (opts.hasHeaderRow() && (ridx == 0)) || (opts.hasFooterRow() && (ridx == len(table)-2)) {
			divider()
		}
	}
}

type cell struct {
	lines []string
	width int
}

func makeCell(s string) cell {
	c := cell{
		lines: strings.Split(s, "\n"),
	}
	for _, l := range c.lines {
		c.width = cmp.MaxInt(c.width, charsWide(l))
	}
	return c
}

func (c *cell) Width() int {
	return c.width
}

func (c *cell) Height() int {
	return len(c.lines)
}

// update the cell by padding it to the supplied size, must be at least as large
// as it currently is
func (c *cell) pad(opts Options, height, width int) {
	for len(c.lines) < height {
		c.lines = append(c.lines, "")
	}
	for i, l := range c.lines {
		lwidth := charsWide(l)
		if lwidth < width {
			pad := strings.Repeat(" ", width-lwidth)
			if opts&RightJustify != 0 {
				c.lines[i] = pad + l
			} else {
				c.lines[i] = l + pad
			}
		}
	}
	c.width = width
}

// charsWide estimates how wide a string will be on a typical terminal or web
// browser. The problem is a bit harder than it appears thanks to Unicode; the
// corresponding unit tests have some interesting cases.
func charsWide(s string) int {
	s = norm.NFC.String(s)
	return utf8.RuneCountInString(s)
}
