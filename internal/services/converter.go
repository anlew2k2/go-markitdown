package services

import "context"

// Converter converts the local file at inputPath to Markdown, writing the
// result to outputPath. Implementations must honor ctx: a cancelled or
// expired ctx must stop the conversion promptly (killing any subprocess it
// holds), not merely return once the operation happens to finish on its
// own.
type Converter interface {
	Convert(ctx context.Context, inputPath, outputPath string) error
}

// CLIConverter adapts ConvertToMarkdownFile (one `markitdown` CLI subprocess
// per call, exactly today's behavior) to the Converter interface. It is the
// default backend (CONVERTER_BACKEND=cli) and the rollback target for
// CONVERTER_BACKEND=persistent.
type CLIConverter struct {
	StderrLimit int
}

func (c CLIConverter) Convert(ctx context.Context, inputPath, outputPath string) error {
	return ConvertToMarkdownFile(ctx, inputPath, outputPath, c.StderrLimit)
}
