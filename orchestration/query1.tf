locals {
  common_tags = {
    Project = "Phase2"
  }
  asg_tags = [
    {
      key                 = "Project"
      value               = "Phase2"
      propagate_at_launch = true
    }
  ]
}

provider "aws" {
  region                      = "us-east-1"
}


resource "aws_security_group" "lg" {
  # HTTP access from anywhere
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = "${local.common_tags}"
}

resource "aws_security_group" "elb_asg" {
  # HTTP access from anywhere
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = "${local.common_tags}"
}

# Step 1: Create launch configuration and Auto Scaling group 
# ==========================================================
resource "aws_launch_configuration" "lc" {
  image_id      = "ami-000a9515fe326bb5b"  
  instance_type = "m5.large"
  security_groups = ["${aws_security_group.elb_asg.id}"]
}

resource "aws_autoscaling_group" "asg" {
  availability_zones = ["us-east-1a"]
  max_size             = 6
  min_size             = 6
  desired_capacity     = 60
  default_cooldown     = 60
  health_check_grace_period = 60
  health_check_type    = "EC2"
  launch_configuration = "${aws_launch_configuration.lc.name}"
  load_balancers       = ["${aws_lb.elb.name}"]
  target_group_arns    = ["${aws_lb_target_group.tg.arn}"]
  tags  = "${local.asg_tags}"
}

# Step 2:
# Create an Application Load Balancer with appropriate listeners and target groups
# ================================================================================
resource "aws_lb" "elb" {
  name               = "LOAD-BALANCER-WEBSERVICE"
  internal           = false
  load_balancer_type = "application"
  security_groups    = ["${aws_security_group.elb_asg.id}"]
  tags               = "${local.asg_tags[0]}"
}

resource "aws_lb_target_group" "tg" {
  name     = "TARGET-GROUP-WEBSERVICE"
  port     = 80
  protocol = "HTTP"
  vpc_id   = "${aws_vpc.vpc.id}"
}

resource "aws_vpc" "vpc" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_lb_listener" "elb_listener" {
  load_balancer_arn = "${aws_lb.elb.arn}"
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = "${aws_lb_target_group.tg.arn}"
  }
}
