#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

INVALID = "invalid_frontend_job_contract"
CHECKOUT = "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
SETUP_NODE = "actions/setup-node@249970729cb0ef3589644e2896645e5dc5ba9c38"
COMMANDS = [
  'test "$(node --version)" = "v22.23.2"',
  'test "$(npm --version)" = "10.9.8"',
  "npm ci",
  "npm exec playwright install --with-deps chromium",
  "npm audit --audit-level=high",
  "npm run lint",
  "npm exec tsc -- --noEmit -p tsconfig.playwright.json",
  "npm test",
  "npm run build",
  "npm run test:browser"
].freeze
REQUIRED_JOBS = %w[frontend-check integration-check python-check rust-check secret-scan].freeze

def invalid?(condition)
  raise INVALID unless condition
end

def validate_frontend_job(workflow)
  invalid?(workflow.is_a?(Hash))
  triggers = workflow["on"] || workflow[true]
  invalid?(triggers.is_a?(Hash))
  push_branches = triggers.dig("push", "branches")
  invalid?(push_branches.is_a?(Array) && push_branches.include?("chore/**"))
  jobs = workflow["jobs"]
  invalid?(jobs.is_a?(Hash))
  job = jobs["frontend-check"]
  invalid?(job.is_a?(Hash))
  invalid?(job["runs-on"] == "ubuntu-24.04")
  invalid?(job.dig("defaults", "run", "working-directory") == "./frontend")
  invalid?(!job.key?("continue-on-error") && !job.key?("if"))
  steps = job["steps"]
  invalid?(steps.is_a?(Array) && steps.all? { |step| step.is_a?(Hash) })
  invalid?(steps.length == 12)
  invalid?(steps[0]["uses"] == CHECKOUT)
  invalid?(steps[0].keys.sort == %w[name uses])
  invalid?(steps[1]["uses"] == SETUP_NODE)
  invalid?(steps[1]["with"] == {
    "node-version-file" => "frontend/.node-version",
    "cache" => "npm",
    "cache-dependency-path" => "frontend/package-lock.json"
  })
  invalid?(steps[1].keys.sort == %w[name uses with])
  invalid?(steps.none? { |step| step.key?("continue-on-error") || step.key?("if") })
  run_steps = steps.drop(2)
  invalid?(run_steps.map { |step| step["run"] } == COMMANDS)
  invalid?(run_steps.all? { |step| step.keys.sort == %w[name run] })
  run_steps.each do |step|
    command = step.fetch("run")
    invalid?(!command.include?("||"))
    invalid?(!command.match?(/&\s*\z/))
    invalid?(!command.match?(/(?:vite|http-server|serve).*&/))
  end
  gate = jobs["refactor-gate"]
  invalid?(gate.is_a?(Hash))
  needs = gate["needs"]
  invalid?(needs.is_a?(Array) && needs == REQUIRED_JOBS)
  nil
rescue RuntimeError => error
  error.message == INVALID ? INVALID : raise
end

def valid_snapshot
  {
    "on" => { "push" => { "branches" => ["develop", "chore/**"] } },
    "jobs" => {
      "frontend-check" => {
        "runs-on" => "ubuntu-24.04",
        "defaults" => { "run" => { "working-directory" => "./frontend" } },
        "steps" => [
          { "name" => "Checkout", "uses" => CHECKOUT },
          {
            "name" => "Set up Node",
            "uses" => SETUP_NODE,
            "with" => {
              "node-version-file" => "frontend/.node-version",
              "cache" => "npm",
              "cache-dependency-path" => "frontend/package-lock.json"
            }
          },
          *COMMANDS.map.with_index { |command, index| { "name" => "Command #{index}", "run" => command } }
        ]
      },
      "refactor-gate" => { "needs" => REQUIRED_JOBS.dup }
    }
  }
end

def self_test
  raise "valid snapshot rejected" unless validate_frontend_job(valid_snapshot).nil?

  mutations = [
    ->(value) { value["on"]["push"]["branches"].delete("chore/**") },
    ->(value) { value["jobs"].delete("frontend-check") },
    ->(value) { value["jobs"]["frontend-check"]["runs-on"] = "ubuntu-latest" },
    ->(value) { value["jobs"]["frontend-check"]["defaults"]["run"]["working-directory"] = "." },
    ->(value) { value["jobs"]["frontend-check"]["continue-on-error"] = true },
    ->(value) { value["jobs"]["frontend-check"]["if"] = "success()" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][0]["uses"] = "actions/checkout@v6" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][1]["uses"] = "actions/setup-node@v6" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][1]["with"]["node-version-file"] = ".node-version" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][1]["with"]["cache"] = "yarn" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][1]["with"]["cache-dependency-path"] = "package-lock.json" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][2]["if"] = "always()" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][2]["run"] += " || true" },
    ->(value) { value["jobs"]["frontend-check"]["steps"][3]["run"] += " &" },
    ->(value) { value["jobs"]["frontend-check"]["steps"].delete_at(4) },
    ->(value) { value["jobs"]["frontend-check"]["steps"][2], value["jobs"]["frontend-check"]["steps"][3] = value["jobs"]["frontend-check"]["steps"][3], value["jobs"]["frontend-check"]["steps"][2] },
    *COMMANDS.each_index.map do |index|
      ->(value) { value["jobs"]["frontend-check"]["steps"][index + 2]["run"] += " changed" }
    end,
    *REQUIRED_JOBS.map do |job_id|
      ->(value) { value["jobs"]["refactor-gate"]["needs"].delete(job_id) }
    end
  ]
  mutations.each_with_index do |mutation, index|
    candidate = Marshal.load(Marshal.dump(valid_snapshot))
    mutation.call(candidate)
    raise "mutation #{index} survived" unless validate_frontend_job(candidate) == INVALID
  end
  puts "component=frontend-ci task=self-test stage=complete disposition=PASS"
end

if ARGV == ["--self-test"]
  self_test
  exit 0
end

abort "usage: validate-frontend-job.rb [--self-test]" unless ARGV.empty?

begin
  workflow = YAML.safe_load(File.binread(".github/workflows/ci.yml"), aliases: false)
rescue Psych::Exception, Errno::ENOENT => error
  warn "component=frontend-ci task=validate stage=failed canonical_error=#{INVALID} detail=#{error.class.name}"
  exit 1
end

if validate_frontend_job(workflow) == INVALID
  warn "component=frontend-ci task=validate stage=failed canonical_error=#{INVALID}"
  exit 1
end
puts "component=frontend-ci task=validate stage=complete disposition=PASS"
