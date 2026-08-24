# Durable release paths

The 2.0.0 package is staged for two independent free paths: a dedicated public code/metadata repository and a Zenodo or Hugging Face dataset record for licence-compatible artefacts. Large or non-commercial components remain separately licensed; pointer-only sources publish checksums and recovery instructions rather than copied source content.

No external path, DOI, or submission is considered live until its provider returns a persistent identifier and the downloaded artifact passes the local manifest. The machine-readable publication manifest therefore remains `staged` until those external actions occur. This prevents a local build from fabricating publication evidence.
