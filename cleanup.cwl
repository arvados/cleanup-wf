cwlVersion: v1.2
class: CommandLineTool

$namespaces:
  arv: "http://arvados.org/cwl#"

requirements:
  DockerRequirement:
    dockerPull: arvados/jobs:2.7.0
  arv:APIRequirement: {}
  WorkReuse:
    enableReuse: false

inputs:
  script:
    type: File
    default:
      class: File
      location: cleanup.py

arguments: [python3, $(inputs.script)]

outputs: []
