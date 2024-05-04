package command


type Command struct {
	name string
	args []string
}

const (
	CRLF = "\r\n"
)

func NewCommand(respArr []string) (*Command, error) {
	cmd := &Command{}
	cmd.name = respArr[0]
	cmd.args = respArr[1:]
	return cmd, nil
}

func (c *Command) CmdToSlice() []string {
	cmd := make([]string, 0)
	cmd = append(cmd, string(c.name))
	cmd = append(cmd, c.args...)
	return cmd
}

func (c *Command) GetArgs() []string {
	return c.args
}

func (c *Command) GetName() string {
	return c.name
}

func (c *Command) GetArg(index int) string {
	return c.args[index]
}