package htsjdk.variant.variantcontext.writer

import java.io.{StringWriter, Writer}

import htsjdk.variant.vcf.VCFHeader

object VCFHeaderWriter {
  // Shim into default visibility VCFWriter class
  def writeHeader(
      header: VCFHeader,
      writer: Writer,
      versionLine: String,
      streamNameForError: String): VCFHeader = {
    VCFWriter.writeHeader(header, writer, versionLine, streamNameForError)
  }

  def writeHeaderAsString(header: VCFHeader): String = {
    val writer = new StringWriter()
    writeHeader(header, writer, VCFWriter.getVersionLine, "headerBuffer")
    writer.toString
  }
}
