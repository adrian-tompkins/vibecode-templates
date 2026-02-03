# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains bootstrap templates for Claude Code projects. Each subdirectory represents a different project template that can be used to quickly set up new projects with best practices and standard configurations.

## Repository Structure

Each template directory contains:
- Complete project structure and boilerplate code
- Configuration files specific to that project type
- A `TEMPLATE.md` file explaining how to use the template
- Example code and patterns following best practices

## Available Templates

### lakeflow-sdp
Bootstrap template for Databricks Spark Declarative Pipelines (DLT) projects. Use this to create new Delta Live Tables pipeline projects with proper structure and configuration.

## Creating a New Template

When adding a new template directory:
1. Create a clear directory structure that reflects the target project layout
2. Include a `TEMPLATE.md` file explaining the template's purpose and usage
3. Add essential configuration files (e.g., databricks.yml, requirements.txt, etc.)
4. Provide example code demonstrating key patterns
5. Update this CLAUDE.md to list the new template

## Working with Templates

When Claude Code is asked to bootstrap a new project using these templates:
1. Copy the relevant template directory to the target location
2. Update project-specific values (names, paths, etc.)
3. Create a project-specific CLAUDE.md based on the template
4. Initialize git repository if needed
