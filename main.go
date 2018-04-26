package main

import (
	"os"
	"fmt"
	"path"
	"github.com/Bitspark/slang/pkg/api"
	"github.com/Bitspark/slang/pkg/core"
	"github.com/Bitspark/slang/pkg/utils"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

type Manifest struct {
	Operator   string          `yaml:"operator"`
	Properties core.Properties `yaml:"properties"`
	Data struct {
		In []interface{} `yaml:"in"`
	} `yaml:"data"`
	ChannelSize    int   `yaml:"channelSize"`
	ChannelDynamic *bool `yaml:"channelDynamic"`
}

func main() {
	manifestFile := "./manifest.yaml"
	if len(os.Args) > 1 {
		manifestFile = os.Args[1]
	}

	manifest := Manifest{}
	manifestBytes, _ := ioutil.ReadFile(manifestFile)
	yaml.Unmarshal(manifestBytes, &manifest)

	if manifest.ChannelSize > 0 {
		core.CHANNEL_SIZE = manifest.ChannelSize
	}

	if manifest.ChannelDynamic != nil {
		core.CHANNEL_DYNAMIC = *manifest.ChannelDynamic
		if core.CHANNEL_DYNAMIC {
			fmt.Println("Channel set dynamic")
		} else {
			fmt.Printf("Channel set static (%d)\n", core.CHANNEL_SIZE)
		}
	}

	slangEnv := api.NewEnviron(path.Dir(manifestFile))
	core.WORKING_DIR = slangEnv.WorkingDir()

	opFilePath := path.Join(slangEnv.WorkingDir(), strings.Replace(manifest.Operator, ".", "/", -1))

	opDefFilePath, err := utils.FileWithFileEnding(opFilePath, api.FILE_ENDINGS)
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}

	def, err := slangEnv.ReadOperatorDef(opDefFilePath, nil)
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}

	specDef, err := core.SpecifyOperator(nil, manifest.Properties, &def)
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}

	op, err := api.CreateAndConnectOperator("", *specDef, false)
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}

	fmt.Print("Compiling " + manifest.Operator + "...")

	// Compile
	compiled, depth := op.Compile()

	fmt.Printf(" done\n")
	fmt.Printf("   operators: %5d\n", len(op.Children()))
	fmt.Printf("    compiled: %5d\n", compiled)
	fmt.Printf("       depth: %5d\n", depth)

	// Connect
	flatDef, err := op.Define()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}

	// Write file
	bytes, _ := yaml.Marshal(flatDef)
	ioutil.WriteFile(path.Join(slangEnv.WorkingDir(), "_slang.yaml"), bytes, 0644)

	// Create and connect the flat operator
	flatOp, err := api.CreateAndConnectOperator("", flatDef, true)
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}

	// Check if all in ports are connected
	err = flatOp.CorrectlyCompiled()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}

	flatOp.Main().Out().Bufferize()
	flatOp.Start()

	fmt.Println("Running...")

	done := make(chan bool)

	go func() {
		for _, obj := range manifest.Data.In {
			flatOp.Main().In().Push(obj)
			fmt.Printf("Push %#v\n", obj)
			done <- true
		}
	}()

	for range manifest.Data.In {
		out := flatOp.Main().Out().Pull()
		fmt.Fprint(os.Stdout, out)
		<-done
	}
}
